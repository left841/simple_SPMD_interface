#ifndef __INSTRUCTION_H__
#define __INSTRUCTION_H__

#include "parallel_defs.h"
#include "message.h"
#include "basic_task.h"
#include "transfer.h"
#include "task_graph.h"
#include <array>
#include <functional>

namespace apl
{

    enum class INSTRUCTION: size_t
    {
        UNDEFINED, END, MES_SEND, MES_RECV, MES_CREATE,
        MES_P_CREATE, TASK_EXE, TASK_CREATE, TASK_RES, ADD_RES_TO_MEMORY,
        MES_DEL, TASK_DEL
    };

    class instruction_block
    {
    protected:
        const size_t* ins;

        template<class T>
        std::vector<T> read_vector(size_t& pos, std::function<T(size_t&)> rf) const;

    public:
        instruction_block(const size_t* const p);
        virtual ~instruction_block();

        instruction_block() = delete;
        instruction_block(const instruction_block&) = delete;
        instruction_block& operator=(const instruction_block&) = delete;

        INSTRUCTION command() const;
        virtual size_t size() const = 0;
    };

    template<class T>
    std::vector<T> instruction_block::read_vector(size_t& pos, std::function<T(size_t&)> rf) const
    {
        std::vector<T> v(ins[pos++]);
        for (T& i: v)
            i = rf(pos);
        return v;
    }

    class instruction_undefined: public instruction_block
    {
    public:
        instruction_undefined(const size_t* const p);

        size_t size() const;
    };

    class instruction_end: public instruction_block
    {
    public:
        instruction_end(const size_t* const p);

        size_t size() const;
    };

    class instruction_message_send: public instruction_block
    {
    public:
        instruction_message_send(const size_t* const p);

        size_t size() const;
        message_id id() const;
        process proc() const;
    };

    class instruction_message_recv: public instruction_block
    {
    public:
        instruction_message_recv(const size_t* const p);

        size_t size() const;
        message_id id() const;
        process proc() const;
    };

    class instruction_message_create: public instruction_block
    {
    public:
        instruction_message_create(const size_t* const p);

        size_t size() const;
        message_id id() const;
        message_type type() const;
    };

    class instruction_message_part_create: public instruction_block
    {
    public:
        instruction_message_part_create(const size_t* const p);

        size_t size() const;
        message_id id() const;
        message_type type() const;
        message_id source() const;
    };

    class instruction_task_execute: public instruction_block
    {
    public:
        instruction_task_execute(const size_t* const p);

        size_t size() const;
        task_id id() const;
    };

    class instruction_task_create: public instruction_block
    {
    public:
        instruction_task_create(const size_t* const p);

        size_t size() const;
        task_id id() const;
        task_type type() const;
        std::vector<message_id> data() const;
        std::vector<message_id> const_data() const;
    };

    class instruction_task_result: public instruction_block
    {
    public:
        instruction_task_result(const size_t* const p);

        size_t size() const;
        task_id id() const;
    };

    class instruction_add_result_to_memory: public instruction_block
    {
    private:
        std::array<size_t, 3> offsets;
    public:
        instruction_add_result_to_memory(const size_t* const p);

        size_t size() const;
        std::vector<message_id> added_messages_init() const;
        std::vector<message_id> added_messages_child() const;
    };

    class instruction_message_delete: public instruction_block
    {
    public:
        instruction_message_delete(const size_t* const p);

        size_t size() const;
        message_id id() const;
    };

    class instruction_task_delete: public instruction_block
    {
    public:
        instruction_task_delete(const size_t* const p);

        size_t size() const;
        perform_id id() const;
    };

    class instruction: public message
    {
    private:

        std::vector<size_t> v;

        void add_cmd(INSTRUCTION id);

    public:

        class block_factory
        {
        private:
            static std::vector<std::function<const instruction_block*(const size_t* const)>> constructors;
        public:
            static const instruction_block* get(const size_t* const p);
        };

        class const_iterator
        {
        private:
            const instruction_block* block;
            const size_t* ins;
        public:
            const_iterator(const size_t* const p);
            ~const_iterator();

            const_iterator& operator++();
            bool operator==(const const_iterator& other);
            bool operator!=(const const_iterator& other);
            const instruction_block& operator*();
        };

        instruction();
        ~instruction();

        void send(const sender& se) const;
        void recv(const receiver& re);

        size_t& operator[](size_t n);
        const size_t& operator[](size_t n) const;

        size_t size();

        void clear();

        void add_end();
        void add_message_sending(message_id id, process proc);
        void add_message_receiving(message_id id, process proc);
        void add_message_creation(message_id id, message_type type);
        void add_message_part_creation(message_id id, message_type type, message_id source);
        void add_task_execution(task_id id);
        void add_task_creation(task_id id, task_type type, std::vector<message_id> data, std::vector<message_id> c_data);
        void add_task_result(task_id id);
        void add_add_result_to_memory(const std::vector<message_id>& mes, const std::vector<message_id>& mes_c);
        void add_message_del(message_id id);
        void add_task_del(perform_id id);

        const_iterator begin() const;
        const_iterator end() const;
    };

}

#endif // __INSTRUCTION_H__
