#ifndef __PARALLELIZER_H__
#define __PARALLELIZER_H__

#include <vector>
#include <queue>
#include <map>
#include <set>
#include <limits>
#include "mpi.h"
#include "parallel_core.h"
#include "instruction.h"
#include "memory_manager.h"
#include "task_graph.h"
#include "intracomm.h"
#include "it_queue.h"

namespace auto_parallel
{

    class parallelizer
    {
    private:

        typedef size_t task_id;
        typedef size_t message_id;

        struct d_info
        {
            message* d;
            int type;
            message::init_info_base* iib;
            message::part_info_base* pib;
            int parent;
            int version;
        };

        struct t_info
        {
            task* t;
            int type;
            int parent;
            int parents;
            int c_childs;
            std::vector<int> childs;
            std::vector<int> data_id;
            std::vector<int> const_data_id;
        };

        intracomm comm;
        intracomm instr_comm;

        it_queue<int> ready_tasks;
        std::vector<t_info> task_v;
        std::vector<d_info> data_v;

        std::vector<int> top_versions;

        void master();
        void worker();

        void send_task_data(int tid, int proc, instruction& ins, std::vector<std::set<int>>& ver, std::vector<std::set<int>>& con);
        void assign_task(int tid, int proc, instruction& ins, std::vector<std::set<int>>& com);
        void send_instruction(int proc, instruction& ins);
        void end_main_task(int tid, task_environment& te, std::vector<std::set<int>>& ver, std::vector<std::set<int>>& con, std::vector<std::set<int>>& con_t);
        void wait_task(int proc, std::vector<std::set<int>>& ver, std::vector<std::set<int>>& con, std::vector<std::set<int>>& con_t);

        void create_message(int id, int type, int proc);
        void create_part(int id, int type, int source, int proc);
        int create_task(int* inst);
        void execute_task(int id);

        void clear();

    public:

        const static int main_proc;

        parallelizer();
        parallelizer(task_graph& _tg);
        ~parallelizer();

        int get_current_proc();
        int get_proc_count();

        void init(task_graph& _tg);

        void execution();

    };

}

#endif // __PARALLELIZER_H__
