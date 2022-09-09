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
#include "apl/comm_group.h"
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
            std::deque<perform_id> ready_tasks;
            std::vector<processes_group> childs_groups;
            std::vector<group_info> group_info_v;
            process parent = MPI_PROC_NULL;
            std::vector<size_t> group_workload;
            size_t all_active_tasks = 0;
            size_t all_tasks = 0;
            bool exe = true;
            size_t internal_comm_size = 0;
            std::vector<size_t> groups_of_processes;
        };

        intracomm internal_comm, external_comm;
        intracomm comm;
        intracomm instr_comm;

        memory_manager2 memory;

        void transfer_state(const std::vector<sub_graph>& sub_graphs, exe_info& info, std::vector<instruction>& ins_v);
        void sub_transfer_state(exe_info& info);
        void init_task_data_sending(perform_id tid, size_t group_id, instruction* inss, exe_info& info);
        void init_message_sending(message_id id, size_t group_id, instruction* inss, exe_info& info);
        bool process_instruction(instruction& ins, exe_info& info, process assigner);
        void match_sendrecv_pairs(const std::vector<instruction>& ins, std::vector<instruction>& outs, exe_info& info);
        std::vector<sub_graph> split_graph(exe_info& info);
        std::vector<sub_graph> split_graph_sub(sub_graph& gr, std::map<perform_id, task_graph_node>& mm, exe_info& info);

        void clear();

        
        std::vector<processes_group> make_topology(size_t tree_param);
        void execute_task(perform_id id, exe_info& info);
        void update_ready_tasks(perform_id tid, exe_info& info);

    public:

        const static process main_proc;

        parallelizer2(const intracomm& _comm = comm_world);
        parallelizer2(task_graph& _tg, const intracomm& _comm = comm_world);
        ~parallelizer2();

        process get_current_proc();
        int get_proc_count();

        void execution(size_t tree_param, task_graph& _tg);
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
