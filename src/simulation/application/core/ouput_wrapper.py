import numpy as np
import pandas as pd


class DsOutputWrapper:
    def __init__(self, passengers, components, nodes, starting_time):
        """
        Output Wrapper
        :param df_pax: original pax data
        :param nodes: list of nodes after simulation
        :param starting_time: starting time of simulation. [time stamp, integer value (total seconds)]
        """
        self.passengers = passengers
        self.components = components
        self.nodes = nodes
        self.starting_time = starting_time

    def _add_column_dt(self, node, arr, col_label, method="normal"):
        if col_label not in self.passengers.columns:
            self.passengers.loc[:, col_label] = pd.NaT

        mask = arr > 0
        passenger_ids = node.passenger_ids[: len(arr[mask])]
        if mask.size > 0 and len(passenger_ids) > 0:
            if method == "normal":
                values_arr = (
                    pd.to_timedelta(arr[mask] - self.starting_time[1], unit="s")
                    + self.starting_time[0]
                )
            elif method == "delta":
                values_arr = arr[mask]

            self.passengers.iloc[
                passenger_ids, self.passengers.columns.get_loc(col_label)
            ] = values_arr

    def _add_column_string(self, node, col_label):
        if col_label not in self.passengers.columns:
            self.passengers.loc[:, col_label] = ""

        passenger_ids = node.passenger_ids
        if len(passenger_ids) > 0:
            self.passengers.iloc[
                passenger_ids, self.passengers.columns.get_loc(col_label)
            ] = node.node_label

    def write_pred(self):
        for n in self.nodes:
            n.passenger_ids = np.array(n.passenger_ids)

            new_col_on = "_".join(n.node_label.split("_")[:-1]) + "_on_pred"
            new_col_done = "_".join(n.node_label.split("_")[:-1]) + "_done_pred"
            new_pt = "_".join(n.node_label.split("_")[:-1]) + "_pt"
            new_col_fac = "_".join(n.node_label.split("_")[:-1]) + "_pred"

            self._add_column_dt(n, n.on_time, new_col_on, method="normal")
            self._add_column_dt(n, n.done_time, new_col_done, method="normal")
            self._add_column_dt(n, n.processing_time, new_pt, method="delta")
            self._add_column_string(n, new_col_fac)

        for component in self.components:
            column1 = f"{component}_pt_pred"
            column2 = f"{component}_done_pred"

            processing_time = pd.to_timedelta(
                self.passengers[f"{component}_pt"], unit="S"
            )

            self.passengers[column1] = self.passengers[column2] - processing_time
