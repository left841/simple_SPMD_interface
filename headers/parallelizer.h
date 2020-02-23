#ifndef __PARALLELIZER_H__
#define __PARALLELIZER_H__

#include <vector>
#include <queue>
#include <map>
#include <set>
#include "mpi.h"
#include "parallel_core.h"
#include "task_graph.h"
#include "intracomm.h"
#include "it_queue.h"

namespace auto_parallel
{

    class parallelizer: public parallel_engine
    {
    private:

        class instruction: public sendable
        {
        public:

            enum class cmd: int
            {
                UNDEFINED, END, MES_SEND, MES_RECV, MES_CREATE,
                MES_P_CREATE, TASK_EXE, TASK_CREATE, TASK_RES
            };

        private:

            std::vector<int> v;
            cmd previous;
            int prev_pos;

            void add_cmd(cmd id);

        public:

            instruction();
            ~instruction();

            void send(const sender& se);
            void recv(const receiver& re);

            int& operator[](size_t n);
            const int& operator[](size_t n) const;

            size_t size();

            void clear();

            void add_end();
            void add_message_sending(int id);
            void add_message_receiving(int id);
            void add_message_creation(int id, int type);
            void add_message_part_creation(int id, int type, int source);
            void add_task_execution(int id);
            void add_task_creation(int id, int type, std::vector<int> data, std::vector<int> c_data);
            void add_task_result(int id, task_environment& env);

        };

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

    public:

        const static int main_proc;

        parallelizer(int* argc = NULL, char*** argv = NULL);
        parallelizer(task_graph& _tg, int* argc = NULL, char*** argv = NULL);
        ~parallelizer();

        int get_current_proc();
        int get_proc_count();

        void init(task_graph& _tg);

        void execution();

    };

}

#endif // __PARALLELIZER_H__
