package com.github.pidan.batch.runtime.event;

public interface TaskEvent extends Event {
    static TaskEvent success(int taskId, Object result) {
        return new TaskSuccess(taskId, result);
    }

    int getTaskId();

    class TaskSuccess implements TaskEvent {
        private final int taskId;
        private final Object result;

        public TaskSuccess(int taskId, Object result) {
            this.taskId = taskId;
            this.result = result;
        }

        @Override
        public int getTaskId() {
            return taskId;
        }

        public Object getTaskResult() {
            return result;
        }
    }
}
