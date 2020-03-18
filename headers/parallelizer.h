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

        intracomm comm;
        intracomm instr_comm;

        std::queue<task_id> ready_tasks;
        //it_queue<task_id> ready_tasks;
        memory_manager memory;

        void master();
        void worker();

        void send_task_data(task_id tid, int proc, instruction& ins, std::vector<std::set<message_id>>& ver, std::vector<std::set<int>>& con);
        void assign_task(task_id tid, int proc, instruction& ins, std::vector<std::set<int>>& com);
        void send_instruction(int proc, instruction& ins);
        void end_main_task(int tid, task_environment& te, std::vector<std::set<message_id>>& ver, std::vector<std::set<int>>& con, std::vector<std::set<int>>& con_t);
        void wait_task(int proc, std::vector<std::set<message_id>>& ver, std::vector<std::set<int>>& con, std::vector<std::set<int>>& con_t);

        void create_message(int id, int type, int proc);
        void create_part(int id, int type, int source, int proc);
        int create_task(int* inst);
        void execute_task(task_id id);

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
