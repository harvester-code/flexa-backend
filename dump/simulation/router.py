from fastapi import APIRouter

from src.simulation.schema import SimulationBody
from src.old_router.simulation._service import SimulationService

simulation_router = APIRouter(prefix="/simulations")
simulation_service = SimulationService()


@simulation_router.post("/run")
def run_simulation(item: SimulationBody):
    result = simulation_service.run_simulation(item)
    return {"message": result}
