#ifndef __PARALLELIZER_H2__
#define __PARALLELIZER_H2__

#include <vector>
#include <queue>
#include <map>
#include <set>
#include <limits>
#include <deque>
#include "mpi.h"
#include "apl/parallel_defs.h"
#include "apl/parallel_core.h"
#include "apl/parallelizer/instruction.h"
#include "apl/parallelizer/memory_manager.h"
#include "apl/task_graph.h"
#include "apl/intracomm.h"
#include "apl/containers/it_queue.h"
#include "apl/parallelizer/graphs.h"

namespace apl
{

    class parallelizer2
    {
    private:

        intracomm comm;
        intracomm instr_comm;

        std::queue<perform_id> ready_tasks;
        std::vector<perform_id> tasks_to_del;
        std::vector<size_t> comm_workload;
        memory_manager memory;

        void send_task_data(perform_id tid, process proc, instruction* inss, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con);
        void send_message(message_id id, process proc, instruction* inss, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con);
        void assign_task(task_id tid, process proc, instruction& ins, std::vector<std::set<perform_id>>& com);
        bool process_instruction(instruction& ins);

        void clear();

        void execution(size_t tree_param);
        std::vector<processes_group> make_topology(size_t tree_param);
        void execute_task(perform_id id);

    public:

        const static process main_proc;

        parallelizer2(const intracomm& _comm = comm_world);
        parallelizer2(task_graph& _tg, const intracomm& _comm = comm_world);
        ~parallelizer2();

        process get_current_proc();
        int get_proc_count();

        void init(task_graph& _tg);

        void execution(task_graph& _tg);
        template<class TaskType, class... InfoTypes, class... ArgTypes>
        void execution(TaskType* root, std::tuple<InfoTypes*...> info, ArgTypes*... args);
    };

    template<class TaskType, class... InfoTypes, class... ArgTypes>
    void parallelizer2::execution(TaskType* root, std::tuple<InfoTypes*...> info, ArgTypes*... args)
    {
        task_graph tg;
        std::vector<message*> info_v, args_v;
        tuple_processors<sizeof...(InfoTypes), InfoTypes...>::create_vector_from_pointers(info_v, info);
        tuple_processors<sizeof...(ArgTypes), typename std::remove_const<ArgTypes>::type...>::create_vector_from_pointers(args_v, std::make_tuple(const_cast<typename std::remove_const<ArgTypes>::type*>(args)...));
        tg.add_task(root, { message_init_factory::get_type<TaskType, InfoTypes...>(), task_factory::get_type<TaskType, ArgTypes...>() }, args_v, info_v);
        execution(tg);
    }

}

#endif // __PARALLELIZER2_H__
