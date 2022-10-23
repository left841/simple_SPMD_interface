#ifndef __PARALLELIZER_H__
#define __PARALLELIZER_H__

#include <vector>
#include <queue>
#include <map>
#include <set>
#include <limits>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <iostream>
#include "mpi.h"
#include "apl/parallel_defs.h"
#include "apl/parallel_core.h"
#include "apl/parallelizer/instruction.h"
#include "apl/parallelizer/memory_manager.h"
#include "apl/task_graph.h"
#include "apl/intracomm.h"
#include "apl/containers/it_queue.h"

namespace apl
{

    class parallelizer
    {
    private:

        struct task_execution_queue_data
        {
            task_id this_task_id;
            task* this_task;
            task_type this_task_type;
            std::vector<message*> args;
            std::vector<const message*> const_args;
        };

        struct finished_task_execution_queue_data
        {
            task_id this_task_id;
            task_environment this_task_environment;
        };

        struct master_exe_info: public message
        {
            std::vector<process> process_workgroup;
            std::vector<std::set<message_id>> contained_messages;
            std::vector<std::set<message_id>> versions_of_messages;
            std::vector<instruction> instructions_for_processes;
            std::map<process, size_t> process_positions;
            std::queue<task_with_process_count> ready_tasks;

            void send(const sender& se) const override;
            void recv(const receiver& re) override;
        };

        intracomm main_comm;
        intracomm instr_comm;
        size_t execution_thread_count;
        process main_proc = 0;

        std::set<std::pair<task_id, size_t>> separate_tasks_set;

        memory_manager memory;

        std::condition_variable exe_threads_cv;
        std::mutex task_queue_mutex;
        std::queue<task_execution_queue_data> task_queue;
        std::mutex finished_task_queue_mutex;
        std::queue<finished_task_execution_queue_data> finished_task_queue;

        void master(master_exe_info& info);
        void worker(size_t current_processes_count);

        void send_task_data(task_id tid, size_t proc, std::vector<size_t>& comm_workload, master_exe_info& info);
        void send_message(message_id id, size_t proc, std::vector<size_t>& comm_workload, master_exe_info& info);
        void send_instruction(instruction& ins);
        void end_main_task(size_t proc, task_id tid, task_environment& te, std::vector<task_id>& tasks_to_del, master_exe_info& info);
        void wait_task(size_t proc, std::vector<task_id>& tasks_to_del, master_exe_info& info);
        void update_ready_tasks(task_id tid, std::vector<task_id>& tasks_to_del, master_exe_info& info);

        void task_execution_thread_function(size_t processes_count);

        void worker_task_finishing(finished_task_execution_queue_data& cur_task_exe_data);
        void execute_task(task_id id);

        void clear();

    public:


        parallelizer(size_t thread_count = 1, const intracomm& _comm = comm_world);
        parallelizer(task_graph& _tg, size_t thread_count = 1, const intracomm& _comm = comm_world);
        ~parallelizer();

        process get_current_proc();
        size_t get_workers_count();

        void init(task_graph& _tg);

        void execution();
        void execution(task_graph& _tg);
        template<class TaskType, class... InfoTypes, class... ArgTypes>
        void execution(TaskType* root, std::tuple<InfoTypes*...> info, ArgTypes*... args);
    };

    template<class TaskType, class... InfoTypes, class... ArgTypes>
    void parallelizer::execution(TaskType* root, std::tuple<InfoTypes*...> info, ArgTypes*... args)
    {
        task_graph tg;
        std::vector<message*> info_v, args_v;
        tuple_processors<sizeof...(InfoTypes), InfoTypes...>::create_vector_from_pointers(info_v, info);
        tuple_processors<sizeof...(ArgTypes), typename std::remove_const<ArgTypes>::type...>::create_vector_from_pointers(args_v, std::make_tuple(const_cast<typename std::remove_const<ArgTypes>::type*>(args)...));
        tg.add_task(root, message_init_factory::get_type<TaskType, InfoTypes...>(), task_factory::get_type<TaskType, ArgTypes...>(), args_v, info_v);
        execution(tg);
    }

}

#endif // __PARALLELIZER_H__
