use crate::{AnalysisPlan, PhysicalPlan, PhysicalStage, TransformStep};
impl PhysicalPlan {
    pub fn has_external_plugins(&self) -> bool {
        self.stages.iter().any(|s|matches!(s,PhysicalStage::Analysis {plan,..} if matches!(plan.as_ref(),AnalysisPlan::External {..})))
    }
    pub fn visit_plugins(
        &self,
        visit: &mut impl FnMut(&std::sync::Arc<sparrow_expr::plugins::Function>),
    ) {
        let mut expr = |e: &sparrow_expr::Expr| e.visit_plugins(visit);
        for stage in &self.stages {
            match stage {
                PhysicalStage::Transform { steps } => {
                    for step in steps {
                        match step {
                            TransformStep::Filter { predicate, .. } => expr(predicate),
                            TransformStep::Project { exprs, .. }
                            | TransformStep::Map { exprs, .. } => {
                                for e in exprs {
                                    expr(e)
                                }
                            }
                        }
                    }
                }
                PhysicalStage::WindowAgg { spec, .. } => {
                    for agg in &spec.aggs {
                        if let Some(e) = &agg.input {
                            expr(e);
                        }
                    }
                }
                PhysicalStage::Route { cases, .. } => {
                    for (e, _) in cases {
                        expr(e);
                    }
                }
                PhysicalStage::Analysis { plan, .. } => {
                    if let AnalysisPlan::Unnest { spec, .. } = plan.as_ref() {
                        expr(&spec.expr);
                    }
                }
                _ => {}
            }
        }
    }
    pub fn has_plugins(&self) -> bool {
        let mut found = false;
        self.visit_plugins(&mut |_| found = true);
        found || self.has_external_plugins()
    }
    pub fn plugin_functions(&self) -> Vec<std::sync::Arc<sparrow_expr::plugins::Function>> {
        let mut pins = Vec::new();
        self.visit_plugins(&mut |p| pins.push(p.clone()));
        pins
    }
}
