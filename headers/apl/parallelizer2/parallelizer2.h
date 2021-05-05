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
#include "apl/parallelizer_shared/instruction.h"
#include "apl/parallelizer2/memory_manager.h"
#include "apl/task_graph.h"
#include "apl/intracomm.h"
#include "apl/containers/it_queue.h"
#include "apl/parallelizer2/graphs.h"

namespace apl
{

    class parallelizer2
    {
    private:

        struct exe_info
        {
            std::vector<std::set<message_id>> versions_mes;
            std::vector<std::set<message_id>> contained_mes;
            std::vector<std::set<perform_id>> contained_tasks;
            std::deque<perform_id> ready_tasks;
            std::vector<processes_group> childs_groups;
            std::vector<group_info> group_info_v;
            process parent = MPI_PROC_NULL;
            std::vector<size_t> group_workload;
            size_t active_graphs = 0;
        };

        intracomm internal_comm, external_comm;
        intracomm comm;
        intracomm instr_comm;

        std::vector<perform_id> tasks_to_del;
        memory_manager2 memory;

        void generate_subgraph_instructions(const std::vector<sub_graph>& sub_graphs, exe_info& info, std::vector<instruction>& ins_v);
        void init_task_data_sending(perform_id tid, size_t group_id, instruction* inss, exe_info& info);
        void init_message_sending(message_id id, size_t group_id, instruction* inss, exe_info& info);
        bool process_instruction(instruction& ins, exe_info& info, process assigner);

        void clear();

        void execution(size_t tree_param, task_graph& _tg);
        std::vector<processes_group> make_topology(size_t tree_param);
        void execute_task(perform_id id, exe_info& info);

    public:

        const static process main_proc;

        parallelizer2(const intracomm& _comm = comm_world);
        parallelizer2(task_graph& _tg, const intracomm& _comm = comm_world);
        ~parallelizer2();

        process get_current_proc();
        int get_proc_count();

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
