use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

pub struct Runner<State, InitFn, TaskFn, OutputFn> {
    initial_state: Arc<State>,
    init_fn: Arc<InitFn>,
    task_fn: Arc<TaskFn>,
    output_fn: OutputFn,
}

impl<State, InitFn, TaskFn, OutputFn, WorkloadFut, Workload, OutputFut, Output>
    Runner<State, InitFn, TaskFn, OutputFn>
where
    State: Send + Sync + 'static,
    InitFn: Send + Sync + 'static + Fn(usize, &State) -> WorkloadFut,
    WorkloadFut: Send + Future<Output = anyhow::Result<Option<Workload>>>,
    Workload: Send,
    TaskFn: Send + Sync + 'static + Fn(u64, &Workload) -> OutputFut,
    OutputFut: Send + Future<Output = anyhow::Result<Option<Output>>>,
    OutputFn: Send + Sync + 'static + FnMut(Output) -> anyhow::Result<()>,
    Output: Send + 'static,
{
    pub fn new(state: State, init_fn: InitFn, task_fn: TaskFn, output_fn: OutputFn) -> Self {
        Runner {
            initial_state: Arc::new(state),
            init_fn: Arc::new(init_fn),
            task_fn: Arc::new(task_fn),
            output_fn,
        }
    }

    pub async fn run(mut self, num_tasks: usize, starting_task_number: u64) -> anyhow::Result<()> {
        const MAX_RETRIES: usize = 5;

        let next_task_num = Arc::new(AtomicU64::new(starting_task_number));
        let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(10);

        // Kick off all of the tasks.
        for task_idx in 0..num_tasks {
            let state = self.initial_state.clone();
            let init_fn = self.init_fn.clone();
            let task_fn = self.task_fn.clone();
            let next_task_num = next_task_num.clone();
            let output_tx = output_tx.clone();

            tokio::spawn(async move {
                let mut current_task_num = next_task_num.fetch_add(1, Ordering::Relaxed);

                'outer: loop {
                    // Don't bothr doing any more if the output chan is closed.
                    if output_tx.is_closed() {
                        return;
                    }

                    // Initialise new workload. This is passed to each task.
                    let workload = match init_fn(task_idx, &state).await {
                        Ok(Some(workload)) => workload,
                        Ok(None) => {
                            // None indicates nothing left to do in this runner.
                            return;
                        }
                        Err(_e) => {
                            // eprintln!("Error instantiating workload for task {task_idx} (running {current_task_num}): {e}");
                            continue;
                        }
                    };

                    // Now, loop running tasks and outputting the results until something goes wrong.
                    let mut task_retries = 0usize;
                    'inner: loop {
                        let output = match task_fn(current_task_num, &workload).await {
                            Ok(Some(output)) => {
                                task_retries = 0;
                                output
                            }
                            Ok(None) => {
                                // None indicates nothing left to do in this runner.
                                return;
                            }
                            Err(e) => {
                                task_retries += 1;
                                if task_retries > MAX_RETRIES {
                                    // task went wrong a few times; re-initialize everything.
                                    eprintln!("Error running task {current_task_num}: {e:?}");
                                    continue 'outer;
                                } else {
                                    // Try task again.
                                    continue 'inner;
                                }
                            }
                        };

                        // Task done; pull the next task ID to run the next task.
                        if let Err(_) = output_tx.send((current_task_num, output)).await {
                            return;
                        }

                        current_task_num = next_task_num.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }

        // Drop the output channel we've held onto here, so that when all of the task-specific
        // clones are dropped, the look below will end.
        drop(output_tx);

        // Here, we wait to gather outputs and run the output fn in order for each output,
        // buffering up any that are received out of order.
        let mut output_task_number = starting_task_number;
        let mut outputs = HashMap::new();
        while let Some((task_num, output)) = output_rx.recv().await {
            if task_num == output_task_number {
                (self.output_fn)(output)?;
                output_task_number += 1;
                // Once we see the output we're looking for, we also check to find as
                // many subsequent outputs we might already have been sent.
                while let Some(output) = outputs.remove(&output_task_number) {
                    (self.output_fn)(output)?;
                    output_task_number += 1;
                }
            } else {
                outputs.insert(task_num, output);
            }
        }

        Ok(())
    }
}

/// A helper which returns the next item from some list each time
/// it's asked for one.
#[derive(Debug, Clone)]
pub struct RoundRobin<T> {
    items: Vec<T>,
    idx: Arc<AtomicUsize>,
}

impl<T> RoundRobin<T> {
    pub fn new(items: Vec<T>) -> Self {
        RoundRobin {
            items,
            idx: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub fn get(&self) -> &T {
        let idx = self.idx.fetch_add(1, Ordering::Relaxed);
        let n = idx % self.items.len();
        &self.items[n]
    }
}
