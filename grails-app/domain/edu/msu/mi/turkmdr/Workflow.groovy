package edu.msu.mi.turkmdr

class Workflow {

    static constraints = {
        name unique: true
    }


    static hasMany = [startingTasks:Task, allTasks:Task]
    static mappedBy = [allTasks:"workflow",startingTasks: "none"]

    String name
    String description
    TaskProperties taskProperties
    Map<String,Task> allTasks = [:]
    List<Task> startingTasks = []


    public Workflow(String name, String description, Map props, Task... tasks) {
        this.name = name;
        this.description = description
        this.taskProperties = new TaskProperties(props)
        this.taskProperties.save()
        initStartingTasks(tasks)



    }

    def initStartingTasks(Task... tasks) {
        tasks.each { task ->
            this.addToStartingTasks(task)
            def taskSet = [task]
            while (!taskSet.empty) {
                Task t = taskSet.pop()
                if (!allTasks.containsKey(t.name)) {
                    allTasks.put(t.name,t)
                    if (t.next) taskSet = (t.next + taskSet) as List
                }
            }
        }
    }



}
