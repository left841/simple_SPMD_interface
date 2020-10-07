#ifndef __PARALLELIZER_H__
#define __PARALLELIZER_H__

#include <vector>
#include <queue>
#include <map>
#include <set>
#include <limits>
#include "mpi.h"
#include "parallel_defs.h"
#include "parallel_core.h"
#include "instruction.h"
#include "memory_manager.h"
#include "task_graph.h"
#include "intracomm.h"
#include "it_queue.h"

namespace apl
{

    class parallelizer
    {
    private:

        intracomm comm;
        intracomm instr_comm;

        std::queue<perform_id> ready_tasks;
        std::vector<perform_id> tasks_to_del;
        memory_manager memory;

        void master();
        void worker();

        void send_task_data(perform_id tid, process proc, instruction& ins, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con);
        void assign_task(task_id tid, process proc, instruction& ins, std::vector<std::set<perform_id>>& com);
        void send_instruction(process proc, instruction& ins);
        void end_main_task(perform_id tid, task_environment& te, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con, std::vector<std::set<perform_id>>& con_t);
        void wait_task(process proc, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con, std::vector<std::set<perform_id>>& con_t);


        void update_ready_tasks(perform_id tid);

        void execute_task(perform_id id);

        void clear();

    public:

        const static process main_proc;

        parallelizer();
        parallelizer(task_graph& _tg);
        ~parallelizer();

        process get_current_proc();
        int get_proc_count();

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
        tuple_processers<sizeof...(InfoTypes), InfoTypes...>::create_vector_from_pointers(info_v, info);
        tuple_processers<sizeof...(ArgTypes), typename std::remove_const<ArgTypes>::type...>::create_vector_from_pointers(args_v, std::make_tuple(const_cast<typename std::remove_const<ArgTypes>::type*>(args)...));
        tg.add_task(root, {message_init_factory::get_type<TaskType, InfoTypes...>(), task_factory::get_type<TaskType, ArgTypes...>()}, args_v, info_v);
        execution(tg);
    }

}

#endif // __PARALLELIZER_H__
