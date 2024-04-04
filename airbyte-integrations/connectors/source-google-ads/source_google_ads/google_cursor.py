from airbyte_cdk.sources.streams.concurrent.cursor import ConcurrentCursor, Comparable
from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition


class GoogleAdsCursor(ConcurrentCursor):

    def close_partition(self, partition: Partition) -> None:
        customer_id = partition.to_slice()["customer_id"]
        slice_count_before = len(self.state.get(customer_id, {}).get("slices", []))
        self._add_slice_to_state(partition)
        if slice_count_before < len(self.state.get(customer_id, {})["slices"]):  # only emit if at least one slice has been processed
            self._merge_partitions(customer_id)
            self._emit_state_message()
        self._has_closed_at_least_one_slice = True

    def _merge_partitions(self, customer_id: str) -> None:
        self.state[customer_id]["slices"] = self._connector_state_converter.merge_intervals(self.state[customer_id]["slices"])

    def _add_slice_to_state(self, partition: Partition) -> None:
        customer_id = partition.to_slice()["customer_id"]
        if self._slice_boundary_fields:
            if "slices" not in self.state[customer_id]:
                raise RuntimeError(
                    f"The state for stream {self._stream_name} should have at least one slice to delineate the sync start time, but no slices are present. This is unexpected. Please contact Support."
                )
            self.state[customer_id]["slices"].append(
                {
                    "start": self._extract_from_slice(partition, self._slice_boundary_fields[self._START_BOUNDARY]),
                    "end": self._extract_from_slice(partition, self._slice_boundary_fields[self._END_BOUNDARY]),
                }
            )
        elif self._most_recent_record:
            raise NotImplementedError()
            if self._has_closed_at_least_one_slice:
                # If we track state value using records cursor field, we can only do that if there is one partition. This is because we save
                # the state every time we close a partition. We assume that if there are multiple slices, they need to be providing
                # boundaries. There are cases where partitions could not have boundaries:
                # * The cursor should be per-partition
                # * The stream state is actually the parent stream state
                # There might be other cases not listed above. Those are not supported today hence the stream should not use this cursor for
                # state management. For the specific user that was affected with this issue, we need to:
                # * Fix state tracking (which is currently broken)
                # * Make the new version available
                # * (Probably) ask the user to reset the stream to avoid data loss
                raise ValueError(
                    "Given that slice_boundary_fields is not defined and that per-partition state is not supported, only one slice is "
                    "expected. Please contact the Airbyte team."
                )

            self.state["slices"].append(
                {
                    self._connector_state_converter.START_KEY: self.start,
                    self._connector_state_converter.END_KEY: self._extract_cursor_value(self._most_recent_record),
                }
            )

