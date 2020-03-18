#ifndef __INSTRUCTION_H__
#define __INSTRUCTION_H__

#include "parallel_defs.h"
#include "message.h"
#include "basic_task.h"
#include "transfer.h"

namespace auto_parallel
{

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
        size_t prev_pos;

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
        void add_task_creation(size_t id, size_t type, std::vector<message_id> data, std::vector<message_id> c_data);
        void add_task_result(int id, task_environment& env);

    };

}

#endif // __INSTRUCTION_H__
