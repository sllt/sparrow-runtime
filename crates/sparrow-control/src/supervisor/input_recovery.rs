use super::*;

impl Supervisor {
    pub async fn recovery_preview(
        &self,
        parent: &str,
        request: crate::recovery_ops::RecoveryRequest,
    ) -> Result<serde_json::Value> {
        let parent = parent.to_string();
        let permit = crate::recovery_ops::PREVIEW_WORK
            .try_acquire()
            .map_err(|_| {
                SparrowError::new(
                    sparrow_model::ErrorCode::ResourceExhausted,
                    "recovery preview concurrency limit reached",
                )
            })?;
        self.catalog(move |s| {
            let _permit = permit;
            crate::recovery_ops::prepare(s, &parent, &request).map(|p| p.view())
        })
        .await
    }
    pub async fn recovery_execute(
        self: &Arc<Self>,
        parent: &str,
        request: crate::recovery_ops::RecoveryRequest,
    ) -> Result<serde_json::Value> {
        let parent = parent.to_string();
        self.transition_operation(false, move |sup| async move {
            if sup.running.lock().await.contains_key(&parent) {
                return Err(crate::input_dlq::invalid("parent is still running"));
            }
            let operation = request.operation.clone();
            let on_error = operation.clone();
            let result = sup
                .catalog(move |s| {
                    if let Some(old) = s.recovery_operation(&operation)? {
                        if old["parent"] != parent
                            || old["request_hash"] != crate::recovery_ops::request_hash(&request)?
                        {
                            return Err(crate::input_dlq::invalid(
                                "operation ID was used for different parameters",
                            ));
                        }
                        if old["phase"] == "ready" {
                            return Ok(old);
                        }
                        if old["phase"] != "preparing" {
                            return Err(crate::input_dlq::invalid("operation was aborted"));
                        }
                    }
                    let mut p = crate::recovery_ops::prepare(s, &parent, &request)?;
                    if request.approve_digest.as_deref() != Some(p.digest.as_str()) {
                        return Err(crate::input_dlq::invalid(
                            "recovery preview changed or approve_digest is missing",
                        ));
                    }
                    let schema = crate::validate::stream_to_schema(&s.get_stream(&p.spec.stream)?)?;
                    let plan = bind_plan_with_store(
                        s,
                        &p.spec,
                        &request.target,
                        if request.mode == "resume" {
                            request.approve_parent_revision + 1
                        } else {
                            1
                        },
                    )?;
                    let policy = store_policy(s, None)?;
                    validate_io_with_plan(
                        &p.spec,
                        &schema,
                        &plan,
                        &StoreSecrets::new(Arc::new(s.clone())),
                        &policy,
                        None,
                    )?;
                    s.reserve_recovery(&p)?;
                    crate::recovery_ops::write_artifact(&mut p)?;
                    s.publish_recovery(&p)?;
                    s.recovery_operation(&operation)?
                        .ok_or_else(|| crate::input_dlq::invalid("published operation missing"))
                })
                .await;
            if let Err(error) = &result {
                let code = error.code;
                let _ = sup
                    .catalog(move |s| s.recovery_error(&on_error, code))
                    .await;
            }
            result
        })
        .await
    }
    pub async fn recovery_finish(
        self: &Arc<Self>,
        parent: &str,
        operation: &str,
    ) -> Result<serde_json::Value> {
        let parent = parent.to_string();
        let operation = operation.to_string();
        self.transition_operation(false,move |sup|async move {
            let (target,dir,end,finished)=sup.catalog({let parent=parent.clone();let operation=operation.clone();move |s| {
                let op=s.recovery_operation(&operation)?.ok_or_else(||crate::input_dlq::invalid("operation missing"))?;
                if op["parent"]!=parent || op["phase"]!="ready" {return Err(crate::input_dlq::invalid("operation is not ready or parent differs"));}
                let target=op["target"].as_str().ok_or_else(||crate::input_dlq::invalid("target missing"))?.to_string();
                let row=s.get_pipeline(&target)?;
                let end=row.spec.source.replay_start.as_ref().and_then(|r|r.end).ok_or_else(||crate::input_dlq::invalid("finish applies only to finite replay operations"))?;
                Ok((target,row.spec.checkpoint_dir.unwrap(),end,op["finished_checkpoint"].as_u64()))
            }}).await?;
            if finished.is_some() {return sup.catalog(move |s|s.recovery_operation(&operation)?.ok_or_else(||crate::input_dlq::invalid("operation missing"))).await;}
            let checkpoint=sup.checkpoint_named(&target).await?;
            let value=sup.catalog({let parent=parent.clone();let operation=operation.clone();move |s| {
                let mut history=CheckpointStore::open_readonly(dir)?;
                let owner=sparrow_model::MemoryOwner::new(ResourceBudget::compact());
                let (snapshot,_credit)=history.recover_pipeline_owned(Some(checkpoint),&owner)?;
                if snapshot.source.offset_bytes!=end {return Err(crate::input_dlq::invalid("replay has not reached its approved end; try finish after progress"));}
                s.finish_recovery(&parent,&operation,checkpoint)
            }}).await?;
            let job=sup.running.lock().await.remove(&target);
            if let Some(job)=job {sup.stop_job(job).await;}
            sup.catalog(move |s| {let actual=s.actual(&target)?;s.set_actual(&target,"stopped",actual.revision,actual.attempt_id.saturating_add(1),None)}).await?;
            Ok(value)
        }).await
    }
    pub async fn recovery_abort(
        self: &Arc<Self>,
        parent: &str,
        id: &str,
        reason: String,
    ) -> Result<serde_json::Value> {
        let parent = parent.to_string();
        let id = id.to_string();
        self.transition_operation(false, move |sup| async move {
            sup.catalog(move |s| s.abort_recovery(&parent, &id, &reason))
                .await
        })
        .await
    }
    pub(super) async fn prepare_input_dlq(
        &self,
        name: &str,
        spec: &crate::PipelineSpec,
    ) -> Result<Option<Arc<crate::input_dlq::InputDlq>>> {
        if spec.source.input_dlq.is_none() {
            return Ok(None);
        }
        self.catalog({
            let name = name.to_string();
            let spec = spec.clone();
            move |s| crate::input_dlq::initialize(s, &name, &spec).map(Some)
        })
        .await
    }

    pub async fn input_dlq_purge(
        self: &Arc<Self>,
        name: &str,
        request: crate::input_dlq::PurgeRequest,
    ) -> Result<serde_json::Value> {
        let name = name.to_string();
        self.transition_operation(false,move |sup|async move {
            if sup.running.lock().await.contains_key(&name) {return Err(crate::input_dlq::invalid("stop the input job before retiring input replay history"));}
            sup.catalog(move |store| {
                let desired=store.desired(&name)?;
                if desired.status!=crate::status::PipelineStatus::Stopped {return Err(crate::input_dlq::invalid("input DLQ purge requires desired stopped"));}
                let row=store.get_pipeline(&name)?;
                let dir=row.spec.checkpoint_dir.ok_or_else(||crate::input_dlq::invalid("checkpoint directory missing"))?;
                sparrow_connectors::check_data_path(Path::new(&dir)).map_err(SparrowError::from)?;
                let mut snapshot_store=CheckpointStore::open_readonly(&dir)?;
                let current=snapshot_store.inventory()?.current.ok_or_else(||crate::input_dlq::invalid("a committed checkpoint is required before purge"))?;
                if current!=request.approve_checkpoint {return Err(crate::input_dlq::invalid("checkpoint changed; inspect and approve CURRENT again"));}
                let owner=sparrow_model::MemoryOwner::new(ResourceBudget::compact());
                let (snapshot,_credit)=snapshot_store.recover_pipeline_owned(Some(current),&owner)?;
                let queue=crate::input_dlq::configured(store,&name)?;
                queue.purge(&request.approve_uuid,request.position,&snapshot.source,request.approve_replay_floor,&request.reason)?;
                Ok(serde_json::json!({"applied":true,"checkpoint_id":current,"input_dlq":queue.status()?}))
            }).await
        }).await
    }
}
use std::path::Path;
