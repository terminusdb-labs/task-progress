use std::sync::{Arc, RwLock};

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum TaskState {
    Init,
    Started,
    Completed,
    Failed,
    Canceled,
}

impl TaskState {
    fn completed(&self) -> bool {
        matches!(
            self,
            TaskState::Completed | TaskState::Failed | TaskState::Canceled
        )
    }
}

#[derive(Clone)]
pub enum SubtaskType {
    Static(usize),
    Dynamic(usize),
}

impl SubtaskType {
    fn amount(&self) -> usize {
        match self {
            Self::Static(a) => *a,
            Self::Dynamic(a) => *a,
        }
    }
}

pub struct TaskProgress {
    total: usize,
    progress: usize,
    expected: usize,
    state: TaskState,
    parent: Option<(SubtaskType, Arc<RwLock<TaskProgress>>)>,
    subtask_progress: Vec<Arc<RwLock<TaskProgress>>>,
}

impl TaskProgress {
    fn start(&mut self) -> Result<(), TaskError> {
        if self.state != TaskState::Init {
            Err(TaskError::AlreadyStarted)
        } else {
            self.state = TaskState::Started;
            Ok(())
        }
    }

    fn progress_from_subtask(this: &Arc<RwLock<Self>>, progress: usize) -> Result<(), TaskError> {
        let mut this_ = this.write().unwrap();
        if this_.state == TaskState::Init {
            Err(TaskError::NotStarted)
        } else if this_.state.completed() {
            Err(TaskError::AlreadyCompleted)
        } else {
            let new_progress = this_.progress + progress;
            if new_progress > this_.total {
                Err(TaskError::ProgressPastTotal)
            } else {
                this_.expected -= progress;
                let old_progress = this_.progress;
                this_.progress = new_progress;
                // update parent progress
                if let Some((parent_total, parent)) = &this_.parent {
                    let parent = parent.clone();
                    let total = this_.total;
                    let parent_total = parent_total.amount();
                    std::mem::drop(this_);
                    let parent_progress = parent_total * new_progress / total;
                    let reported_progress = parent_total * old_progress / total;
                    let dif = parent_progress - reported_progress;
                    TaskProgress::progress_from_subtask(&parent, dif)?;
                }
                Ok(())
            }
        }
    }

    fn progress(this: &Arc<RwLock<Self>>, progress: usize) -> Result<(), TaskError> {
        let mut this_ = this.write().unwrap();
        if this_.state == TaskState::Init {
            Err(TaskError::NotStarted)
        } else if this_.state.completed() {
            Err(TaskError::AlreadyCompleted)
        } else {
            let new_progress = this_.progress + progress;
            if new_progress > this_.total {
                Err(TaskError::ProgressPastTotal)
            } else {
                let old_progress = this_.progress;
                this_.progress = new_progress;
                // update parent progress
                if let Some((parent_total, parent)) = &this_.parent {
                    let parent = parent.clone();
                    let total = this_.total;
                    let parent_total = parent_total.amount();
                    std::mem::drop(this_);
                    let parent_progress = parent_total * new_progress / total;
                    let reported_progress = parent_total * old_progress / total;
                    let dif = parent_progress - reported_progress;
                    TaskProgress::progress_from_subtask(&parent, dif)?;
                }
                Ok(())
            }
        }
    }

    fn cancel_uncompleted_subtasks(&self) -> Result<(), TaskError> {
        // we're presumably doing this while holding a lock for our current state.
        // This won't deadlock, as this only descends deeper and each
        // subtask is uniquely owned by only one task. As such, no
        // cycles can occur in this descent.
        // That said, it'd be better to change this into a version
        // that does drop locks quickly and iteratively deepens, as
        // this will ensure pollers don't have to sleep long.
        for subtask in self.subtask_progress.iter() {
            let mut subtask = subtask.write().unwrap();
            if !subtask.state.completed() {
                subtask.cancel_()?;
            }
        }

        Ok(())
    }

    fn complete(this: &Arc<RwLock<Self>>) -> Result<(), TaskError> {
        let mut this_ = this.write().unwrap();
        if this_.state.completed() {
            Err(TaskError::AlreadyCompleted)
        } else if this_.state == TaskState::Init {
            Err(TaskError::NotStarted)
        } else {
            this_.state = TaskState::Completed;
            this_.cancel_uncompleted_subtasks()?;
            let old_progress = this_.progress;
            this_.progress = this_.total;
            if let Some((parent_total, parent)) = this_.parent.clone() {
                let parent_progress = parent_total.amount();
                let reported_progress = parent_total.amount() * old_progress / this_.total;
                let dif = parent_progress - reported_progress;

                // drop our current lock to prevent deadlocks
                std::mem::drop(this_);

                // complete the progress bar
                TaskProgress::progress_from_subtask(&parent, dif)?;
            }

            Ok(())
        }
    }

    fn fail(this: &Arc<RwLock<Self>>) -> Result<(), TaskError> {
        let mut this_ = this.write().unwrap();
        if this_.state.completed() {
            Err(TaskError::AlreadyCompleted)
        } else if this_.state == TaskState::Init {
            Err(TaskError::NotStarted)
        } else {
            this_.state = TaskState::Failed;
            this_.cancel_uncompleted_subtasks()?;
            if let Some((_progress, parent)) = this_.parent.clone() {
                // drop our current lock to prevent deadlocks
                std::mem::drop(this_);
                TaskProgress::fail(&parent)?;
            }
            Ok(())
        }
    }

    fn fail_if_uncompleted(this: &Arc<RwLock<Self>>) {
        if let Ok(mut this_) = this.try_write() {
            if !this_.state.completed() && this_.state != TaskState::Init {
                this_.state = TaskState::Failed;
                let _ = this_.cancel_uncompleted_subtasks();
                if let Some((_progress, parent)) = this_.parent.clone() {
                    // drop our current lock to prevent deadlocks
                    std::mem::drop(this_);
                    TaskProgress::fail_if_uncompleted(&parent);
                }
            }
        }
    }

    fn cancel_(&mut self) -> Result<(), TaskError> {
        if self.state.completed() {
            Err(TaskError::AlreadyCompleted)
        } else {
            self.cancel_uncompleted_subtasks()?;
            self.state = TaskState::Canceled;

            Ok(())
        }
    }

    fn cancel(this: &Arc<RwLock<Self>>) -> Result<(), TaskError> {
        let mut this_ = this.write().unwrap();
        this_.cancel_()?;

        if let Some((progress, parent)) = this_.parent.clone() {
            if let SubtaskType::Dynamic(progress) = progress {
                // drop our current lock to prevent deadlocks
                std::mem::drop(this_);

                // after a cancel, the parent shouldn't be expecting
                // this work anymore.
                // This allows us to repeatedly spawn and cancel
                // subtasks without running out of progress.
                let mut guard = parent.write().unwrap();
                guard.expected -= progress;
            }
        }

        Ok(())
    }

    /// Start a static subtask.
    ///
    /// A static subtask is one we knew would be started when this
    /// task was made. As such, its expected progress has already been
    /// recorded and it should always be ok to create this task, as
    /// long as this task is in the started state.
    fn start_static_subtask(
        &mut self,
        progress: Arc<RwLock<TaskProgress>>,
    ) -> Result<(), TaskError> {
        if self.state.completed() {
            Err(TaskError::AlreadyCompleted)
        } else if self.state == TaskState::Init {
            Err(TaskError::NotStarted)
        } else {
            self.subtask_progress.push(progress);
            Ok(())
        }
    }

    /// Start a dynamic subtask.
    ///
    /// A dynamic subtask is one we didn't know we would need when
    /// this task was made. As such, we need to be careful that we
    /// don't exceed the total allowed progress.
    fn start_dynamic_subtask(
        &mut self,
        progress: Arc<RwLock<TaskProgress>>,
        size: usize,
    ) -> Result<(), TaskError> {
        if self.state.completed() {
            Err(TaskError::AlreadyCompleted)
        } else if self.state == TaskState::Init {
            Err(TaskError::NotStarted)
        } else {
            let new_expected = self.expected + size;
            if new_expected > self.total {
                Err(TaskError::ExpectedPastTotal)
            } else {
                self.subtask_progress.push(progress);
                Ok(())
            }
        }
    }
}

#[derive(Clone)]
pub struct Task {
    progress: Arc<RwLock<TaskProgress>>,
    subtasks: Arc<Vec<(usize, Task)>>,
}

#[derive(Debug, Clone, Copy)]
pub enum TaskError {
    AlreadyStarted,
    AlreadyCompleted,
    NotStarted,
    ProgressPastTotal,
    ExpectedPastTotal,
    NoSubtaskWithId,
}

impl Task {
    pub fn new(total: usize, mut subtasks: Vec<(usize, Task)>) -> Self {
        assert!(
            subtasks.iter().map(|(x, _)| *x).sum::<usize>() <= total,
            "total subtasks size is greater than the total work given"
        );
        let progress = Arc::new(RwLock::new(TaskProgress {
            total,
            expected: subtasks.iter().map(|(size, _)| size).sum::<usize>(),
            progress: 0,
            state: TaskState::Init,
            parent: None,
            subtask_progress: Vec::new(),
        }));
        for (size, subtask) in subtasks.iter_mut() {
            let mut sub_progress = subtask.progress.write().unwrap();
            if sub_progress.parent.is_some() {
                std::mem::drop(sub_progress);
                panic!("subtask already had a parent");
            } else if sub_progress.state != TaskState::Init {
                std::mem::drop(sub_progress);
                panic!("subtask was already started");
            }
            sub_progress.parent = Some((SubtaskType::Static(*size), progress.clone()));
        }
        Self {
            progress,

            subtasks: Arc::new(subtasks),
        }
    }

    pub fn start(&self) -> Result<StartedTask, TaskError> {
        let mut progress = self.progress.write().unwrap();
        TaskProgress::start(&mut progress)?;

        Ok(StartedTask { task: self.clone() })
    }

    pub fn current_progress(&self) -> usize {
        let guard = self.progress.read().unwrap();
        guard.progress
    }
}

pub struct StartedTask {
    task: Task,
}

impl Drop for StartedTask {
    fn drop(&mut self) {
        TaskProgress::fail_if_uncompleted(&self.task.progress);
    }
}

impl StartedTask {
    pub fn progress(&self, amount: usize) -> Result<(), TaskError> {
        TaskProgress::progress(&self.task.progress, amount)
    }
    pub fn complete(self) -> Result<(), TaskError> {
        TaskProgress::complete(&self.task.progress)
    }
    pub fn fail(self) -> Result<(), TaskError> {
        TaskProgress::fail(&self.task.progress)
    }
    pub fn cancel(self) -> Result<(), TaskError> {
        TaskProgress::cancel(&self.task.progress)
    }

    pub fn current_progress(&self) -> usize {
        self.task.current_progress()
    }

    pub fn start_subtask(&self, id: usize) -> Result<StartedTask, TaskError> {
        if let Some((_size, subtask)) = self.task.subtasks.get(id) {
            let started = subtask.start()?;
            let mut guard = self.task.progress.write().unwrap();
            let progress = subtask.progress.clone();
            guard.start_static_subtask(progress)?;
            Ok(started)
        } else {
            Err(TaskError::NoSubtaskWithId)
        }
    }

    pub fn start_dynamic_subtask(
        &self,
        size: usize,
        inner_size: usize,
        subtasks: Vec<(usize, Task)>,
    ) -> Result<StartedTask, TaskError> {
        let subtask = Task::new(inner_size, subtasks);
        subtask.start()?;
        let mut guard = subtask.progress.write().unwrap();
        guard.parent = Some((SubtaskType::Dynamic(size), self.task.progress.clone()));
        guard.start_dynamic_subtask(subtask.progress.clone(), size)?;
        std::mem::drop(guard);

        Ok(StartedTask { task: subtask })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_task_progress() {
        let task = Task::new(100, vec![]);
        let started = task.start().unwrap();

        assert_eq!(0, task.current_progress());
        assert_eq!(0, started.current_progress());
        started.progress(15).unwrap();
        assert_eq!(15, task.current_progress());
        assert_eq!(15, started.current_progress());
        started.progress(20).unwrap();
        assert_eq!(35, task.current_progress());
        assert_eq!(35, started.current_progress());
        started.complete().unwrap();
        assert_eq!(100, task.current_progress());
    }

    #[test]
    fn nested_task_progress() {
        let task = Task::new(100, vec![(50, Task::new(100, vec![]))]);
        let started = task.start().unwrap();

        assert_eq!(0, task.current_progress());
        assert_eq!(0, started.current_progress());
        started.progress(15).unwrap();
        assert_eq!(15, task.current_progress());
        assert_eq!(15, started.current_progress());

        let (_, subtask) = &task.subtasks[0];
        let started_subtask = started.start_subtask(0).unwrap();
        assert_eq!(15, started.current_progress());
        assert_eq!(0, subtask.current_progress());
        assert_eq!(0, started_subtask.current_progress());

        started_subtask.progress(50).unwrap();
        assert_eq!(40, started.current_progress());
        assert_eq!(50, subtask.current_progress());
        assert_eq!(50, started_subtask.current_progress());

        started_subtask.complete().unwrap();
        assert_eq!(65, started.current_progress());
        assert_eq!(100, subtask.current_progress());
    }

    #[test]
    fn twice_nested_task_progress() {
        let task = Task::new(
            128,
            vec![(64, Task::new(128, vec![(64, Task::new(128, vec![]))]))],
        );
        let started = task.start().unwrap();
        let (_, subtask) = &task.subtasks[0];
        let started_subtask = started.start_subtask(0).unwrap();
        let (_, subsubtask) = &subtask.subtasks[0];
        let started_subsubtask = started_subtask.start_subtask(0).unwrap();
        started_subsubtask.progress(64).unwrap();
        assert_eq!(64, started_subsubtask.current_progress());
        assert_eq!(32, started_subtask.current_progress());
        assert_eq!(16, started.current_progress());

        started_subsubtask.complete().unwrap();
        assert_eq!(128, subsubtask.current_progress());
        assert_eq!(64, started_subtask.current_progress());
        assert_eq!(32, started.current_progress());

        started_subtask.complete().unwrap();
        assert_eq!(128, subsubtask.current_progress());
        assert_eq!(128, subtask.current_progress());
        assert_eq!(64, started.current_progress());

        started.complete().unwrap();
        assert_eq!(128, subsubtask.current_progress());
        assert_eq!(128, subtask.current_progress());
        assert_eq!(128, task.current_progress());
    }
}
