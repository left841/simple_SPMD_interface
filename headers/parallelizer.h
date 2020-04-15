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

        std::queue<task_id> ready_tasks;
        memory_manager memory;

        void master();
        void worker();

        void send_task_data(task_id tid, process proc, instruction& ins, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con);
        void assign_task(task_id tid, process proc, instruction& ins, std::vector<std::set<task_id>>& com);
        void send_instruction(process proc, instruction& ins);
        void end_main_task(task_id tid, task_environment& te, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con, std::vector<std::set<task_id>>& con_t);
        void wait_task(process proc, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con, std::vector<std::set<task_id>>& con_t);


        void update_ready_tasks(task_id tid);

        void execute_task(task_id id);

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
        void execution(task* root);
    };

}

#endif // __PARALLELIZER_H__
