use crate::execution_plans::MetricsWrapperExec;
use crate::{NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec, Stage};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// This trait represents a node that introduces the necessity of a network boundary in the plan.
/// The distributed planner, upon stepping into one of these, will break the plan and build a stage
/// out of it.
pub trait NetworkBoundary: ExecutionPlan {
    /// Called when a [Stage] is correctly formed. The [NetworkBoundary] can use this
    /// information to perform any internal transformations necessary for distributed execution.
    ///
    /// Typically, [NetworkBoundary]s will use this call for transitioning from "Pending" to "ready".
    fn with_input_stage(
        &self,
        input_stage: Stage,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>>;

    /// Returns the assigned input [Stage], if any.
    fn input_stage(&self) -> &Stage;
}

/// Extension trait for downcasting dynamic types to [NetworkBoundary].
pub trait NetworkBoundaryExt {
    /// Downcasts self to a [NetworkBoundary] if possible.
    fn as_network_boundary(&self) -> Option<&dyn NetworkBoundary>;
    /// Returns whether self is a [NetworkBoundary] or not.
    fn is_network_boundary(&self) -> bool {
        self.as_network_boundary().is_some()
    }
}

impl NetworkBoundaryExt for dyn ExecutionPlan {
    fn as_network_boundary(&self) -> Option<&dyn NetworkBoundary> {
        // Look through MetricsWrapperExec wrappers to find the underlying network boundary.
        // This is necessary because the metrics rewriter wraps all nodes, including network
        // boundaries, in MetricsWrapperExec before the second pass that processes child stages.
        if let Some(wrapper) = self.downcast_ref::<MetricsWrapperExec>() {
            return wrapper.inner().as_network_boundary();
        }

        if let Some(node) = self.downcast_ref::<NetworkShuffleExec>() {
            Some(node)
        } else if let Some(node) = self.downcast_ref::<NetworkCoalesceExec>() {
            Some(node)
        } else if let Some(node) = self.downcast_ref::<NetworkBroadcastExec>() {
            Some(node)
        } else {
            None
        }
    }
}
