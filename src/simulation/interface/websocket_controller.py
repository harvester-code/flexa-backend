import asyncio
import json

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect

from src.containers import Container
from src.database import get_snowflake_session
from src.middleware import websocket_jwt_decoder
from src.simulation.application.service import SimulationService
from src.simulation.interface.schema import RunSimulationBody

ws_router = APIRouter()


async def long_running_task(websocket: WebSocket):
    total_steps = 10
    for step in range(total_steps):
        # 작업의 각 단계에서 필요한 처리를 진행합니다.
        await asyncio.sleep(1)  # 예시로 1초씩 대기 (실제 작업 로직 대체)
        progress = int((step + 1) / total_steps * 100)
        # 진행 상황을 JSON 형식으로 전송합니다.
        await websocket.send_json({"progress": progress})
    # 작업이 완료되면 완료 메시지 전송
    await websocket.send_text("Task complete")


@ws_router.websocket("/test")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    await long_running_task(websocket)
    await websocket.close()


@ws_router.websocket("/simulation")
@inject
async def run_simulation(
    websocket: WebSocket,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    db=Depends(get_snowflake_session),
):
    if await websocket_jwt_decoder(websocket) is None:  # 인증 실패 시
        return

    try:
        while True:
            data = await websocket.receive_text()
            await websocket.send_json({"progress": "0%"})
            await asyncio.sleep(0.1)

            try:
                parsed_data = json.loads(data)
                run_simulation_data = RunSimulationBody(**parsed_data)

                user_id = "websocket.state.user_id"
                if not user_id:
                    await websocket.send_json({"error": "User ID is required"})
                    raise

                result = await simulation_service.run_simulation(
                    websocket,
                    db,
                    user_id,
                    run_simulation_data.scenario_id,
                    run_simulation_data.flight_schedule,
                    run_simulation_data.destribution_conditions,
                    run_simulation_data.processes,
                    run_simulation_data.components,
                )

                # ✅ 완료 메시지 전송
                await websocket.send_json({"progress": "100%"})

                await websocket.send_json(
                    {"message": "Simulation completed", "result": result}
                )

            except Exception as e:
                await websocket.send_json({"error": str(e)})
                raise

    except WebSocketDisconnect:
        await websocket.send_json({"error": "dd"})
        print("Client disconnected")
    finally:
        await websocket.close()
