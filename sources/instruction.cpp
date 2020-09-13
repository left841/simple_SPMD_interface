#include "instruction.h"

namespace apl
{

    instruction_block::instruction_block(const size_t* const p): ins(p)
    { }

    instruction_block::~instruction_block()
    { }

    INSTRUCTION instruction_block::command() const
    { return static_cast<INSTRUCTION>(ins[0]); }

    instruction::instruction(): sendable()
    { }

    instruction::~instruction()
    { }

    void instruction::send(const sender& se) const
    {
        se.send(v.data(), static_cast<int>(v.size()));
    }

    void instruction::recv(const receiver& re)
    {
        v.resize(re.probe<size_t>());
        re.recv(v.data(), static_cast<int>(v.size()));
    }

    size_t& instruction::operator[](size_t n)
    { return v[n]; }

    const size_t& instruction::operator[](size_t n) const
    { return v[n]; }

    size_t instruction::size()
    { return v.size(); }

    void instruction::clear()
    { v.clear(); }

    void instruction::add_cmd(INSTRUCTION id)
    { v.push_back(static_cast<size_t>(id)); }

    std::vector<std::function<const instruction_block* (const size_t* const)>> instruction::block_factory::constructors =
    {
        [](const size_t* const p)->const instruction_block*
        { return new instruction_undefined(p); },

        [](const size_t* const p)->const instruction_block*
        { return new instruction_end(p); },

        [](const size_t* const p)->const instruction_block*
        { return new instruction_message_send(p); },

        [](const size_t* const p)->const instruction_block*
        { return new instruction_message_recv(p); },

        [](const size_t* const p)->const instruction_block*
        { return new instruction_message_create(p); },

        [](const size_t* const p)->const instruction_block*
        { return new instruction_message_part_create(p); },

        [](const size_t* const p)->const instruction_block*
        { return new instruction_task_execute(p); },

        [](const size_t* const p)->const instruction_block*
        { return new instruction_task_create(p); },

        [](const size_t* const p)->const instruction_block*
        { return new instruction_task_result(p); },

        [](const size_t* const p)->const instruction_block*
        { return new instruction_add_result_to_memory(p); }
    };

    const instruction_block* instruction::block_factory::get(const size_t* const p)
    { return constructors[*p](p); }

    instruction::const_iterator::const_iterator(const size_t* const p): ins(p), block(nullptr)
    { }

    instruction::const_iterator::~const_iterator()
    {
        if (block != nullptr)
            delete block;
    }

    instruction::const_iterator& instruction::const_iterator::operator++()
    {
        if (block == nullptr)
            block = block_factory::get(ins);
        ins += block->size();
        delete block;
        block = nullptr;
        return *this;
    }

    bool instruction::const_iterator::operator==(const const_iterator& other)
    { return ins == other.ins; }

    bool instruction::const_iterator::operator!=(const const_iterator& other)
    { return ins != other.ins; }

    const instruction_block& instruction::const_iterator::operator*()
    {
        if (block == nullptr)
            block = block_factory::get(ins);
        return *block;
    }

    instruction::const_iterator instruction::begin() const
    { return const_iterator(v.data()); }

    instruction::const_iterator instruction::end() const
    { return const_iterator(v.data() + v.size()); }

    // UNDEFINED
    instruction_undefined::instruction_undefined(const size_t* const p): instruction_block(p)
    { }

    size_t instruction_undefined::size() const
    { return 1; }

    // END
    instruction_end::instruction_end(const size_t* const p): instruction_block(p)
    { }

    size_t instruction_end::size() const
    { return 1; }

    void instruction::add_end()
    { add_cmd(INSTRUCTION::END); }

    // MES_SEND
    instruction_message_send::instruction_message_send(const size_t* const p): instruction_block(p)
    { }

    size_t instruction_message_send::size() const
    { return 2; }

    message_id instruction_message_send::id() const
    { return static_cast<message_id>(ins[1]); }

    void instruction::add_message_sending(message_id id)
    {
        add_cmd(INSTRUCTION::MES_SEND);
        v.push_back(id);
    }

    // MES_RECV
    instruction_message_recv::instruction_message_recv(const size_t* const p): instruction_block(p)
    { }

    size_t instruction_message_recv::size() const
    { return 2; }

    message_id instruction_message_recv::id() const
    { return static_cast<message_id>(ins[1]); }

    void instruction::add_message_receiving(message_id id)
    {
        add_cmd(INSTRUCTION::MES_RECV);
        v.push_back(id);
    }

    // MES_CREATE
    instruction_message_create::instruction_message_create(const size_t* const p): instruction_block(p)
    { }

    size_t instruction_message_create::size() const
    { return 3; }

    message_id instruction_message_create::id() const
    { return static_cast<message_id>(ins[1]); }

    size_t instruction_message_create::type() const
    { return ins[2]; }

    void instruction::add_message_creation(message_id id, message_type type)
    {
        add_cmd(INSTRUCTION::MES_CREATE);
        v.push_back(id);
        v.push_back(type);
    }

    // MES_P_CREATE
    instruction_message_part_create::instruction_message_part_create(const size_t* const p): instruction_block(p)
    { }

    size_t instruction_message_part_create::size() const
    { return 4; }

    message_id instruction_message_part_create::id() const
    { return static_cast<message_id>(ins[1]); }

    size_t instruction_message_part_create::type() const
    { return ins[2]; }

    message_id instruction_message_part_create::source() const
    { return ins[3]; }

    void instruction::add_message_part_creation(message_id id, message_type type, message_id source)
    {
        add_cmd(INSTRUCTION::MES_P_CREATE);
        v.push_back(id);
        v.push_back(type);
        v.push_back(source);
    }

    // TASK_EXE
    instruction_task_execute::instruction_task_execute(const size_t* const p): instruction_block(p)
    { }

    size_t instruction_task_execute::size() const
    { return 2; }

    task_id instruction_task_execute::id() const
    { return static_cast<task_id>(ins[1]); }

    void instruction::add_task_execution(task_id id)
    {
        add_cmd(INSTRUCTION::TASK_EXE);
        v.push_back(id);
    }

    // TASK_CREATE
    instruction_task_create::instruction_task_create(const size_t* const p): instruction_block(p)
    { }

    size_t instruction_task_create::size() const
    {
        size_t n = ins[3];
        return 5 + n + ins[4 + n];
    }

    task_id instruction_task_create::id() const
    { return static_cast<task_id>(ins[1]); }

    size_t instruction_task_create::type() const
    { return ins[2]; }

    std::vector<message_id> instruction_task_create::data() const
    {
        std::vector<message_id> v(ins[3]);
        for (size_t i = 0; i < v.size(); ++i)
            v[i] = ins[4 + i];
        return v;
    }

    std::vector<message_id> instruction_task_create::const_data() const
    {
        std::vector<message_id> v(ins[4 + ins[3]]);
        for (size_t i = 0; i < v.size(); ++i)
            v[i] = ins[5 + ins[3] + i];
        return v;
    }

    void instruction::add_task_creation(task_id id, task_type type, std::vector<message_id> data, std::vector<message_id> c_data)
    {
        add_cmd(INSTRUCTION::TASK_CREATE);
        v.push_back(id);
        v.push_back(type);
        v.push_back(data.size());
        for (message_id i: data)
            v.push_back(i);
        v.push_back(c_data.size());
        for (message_id i: c_data)
            v.push_back(i);
    }

    // TASK_RES
    instruction_task_result::instruction_task_result(const size_t* const p): instruction_block(p)
    { }

    size_t instruction_task_result::size() const
    { return 2; }

    task_id instruction_task_result::id() const
    { return static_cast<task_id>(ins[1]); }

    void instruction::add_task_result(task_id id)
    {
        add_cmd(INSTRUCTION::TASK_RES);
        v.push_back(id);
    }

    // ADD_RES_TO_MEMORY
    instruction_add_result_to_memory::instruction_add_result_to_memory(const size_t* const p): instruction_block(p)
    {
        offsets[0] = 1;
        offsets[1] = offsets[0] + ins[offsets[0]] + 1;
        offsets[2] = offsets[1] + ins[offsets[1]] + 1;
        offsets[3] = offsets[2] + ins[offsets[2]] + 1;
        offsets[4] = offsets[3] + ins[offsets[3]] + 1;
    }

    size_t instruction_add_result_to_memory::size() const
    { return offsets[4]; }

    std::vector<message_id> instruction_add_result_to_memory::added_messages_init() const
    {
        size_t pos = offsets[0];
        std::vector<message_id> v = read_vector<message_id>(pos, [this](size_t& p)->message_id
        {
            return ins[p++];
        });
        return v;
    }

    std::vector<message_id> instruction_add_result_to_memory::added_messages_child() const
    {
        size_t pos = offsets[1];
        std::vector<message_id> v = read_vector<message_id>(pos, [this](size_t& p)->message_id
        {
            return ins[p++];
        });
        return v;
    }

    std::vector<task_id> instruction_add_result_to_memory::added_tasks_simple() const
    {
        size_t pos = offsets[2];
        std::vector<task_id> v = read_vector<task_id>(pos, [this](size_t& p)->task_id
        {
            return ins[p++];
        });
        return v;
    }

    std::vector<task_id> instruction_add_result_to_memory::added_tasks_child() const
    {
        size_t pos = offsets[3];
        std::vector<task_id> v = read_vector<task_id>(pos, [this](size_t& p)->task_id
        {
            return ins[p++];
        });
        return v;
    }

    void instruction::add_add_result_to_memory(const std::vector<message_id>& mes, const std::vector<message_id>& mes_c, const std::vector<task_id>& tasks, const std::vector<task_id>& tasks_c)
    {
        add_cmd(INSTRUCTION::ADD_RES_TO_MEMORY);
        v.push_back(mes.size());
        for (message_id i: mes)
            v.push_back(i);
        v.push_back(mes_c.size());
        for (message_id i: mes_c)
            v.push_back(i);
        v.push_back(tasks.size());
        for (task_id i: tasks)
            v.push_back(i);
        v.push_back(tasks_c.size());
        for (task_id i: tasks_c)
            v.push_back(i);
    }

}
