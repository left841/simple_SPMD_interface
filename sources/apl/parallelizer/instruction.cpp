#include "apl/parallelizer/instruction.h"

namespace apl
{

    instruction_block::instruction_block(const size_t* const p): ins(p)
    { }

    instruction_block::~instruction_block()
    { }

    INSTRUCTION instruction_block::command() const
    { return static_cast<INSTRUCTION>(ins[0]); }

    template<>
    message_id instruction_block::read<message_id>(size_t& pos) const
    {
        message_id val{ins[pos], static_cast<process>(ins[pos + 1])};
        pos += 2;
        return val;
    }

    template<>
    std::pair<message_id, message_id> instruction_block::read<std::pair<message_id, message_id>>(size_t& pos) const
    {
        pos += 4;
        return {{ins[pos - 4], static_cast<process>(ins[pos - 3])}, {ins[pos - 2], static_cast<process>(ins[pos - 1])}};
    }

    template<>
    void instruction::write<message_id>(const message_id& val)
    {
        v.push_back(val.num);
        v.push_back(val.proc);
    }

    template<>
    void instruction::write<task_id>(const task_id& val)
    {
        v.push_back(val.num);
        v.push_back(val.proc);
    }

    template<>
    void instruction::write<std::pair<message_id, message_id>>(const std::pair<message_id, message_id>& val)
    {
        write(val.first);
        write(val.second);
    }

    instruction::instruction(): message()
    { }

    instruction::~instruction()
    { }

    void instruction::send(const sender& se) const
    { se.send(v.data(), v.size()); }

    void instruction::recv(const receiver& re)
    {
        v.resize(re.probe<size_t>());
        re.recv(v.data(), v.size());
    }

    void instruction::isend(const sender& se, request_block& req) const
    { se.isend(v.data(), v.size(), req); }

    void instruction::irecv(const receiver& re, request_block& req)
    {
        v.resize(re.probe<size_t>());
        re.irecv(v.data(), v.size(), req);
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
        { return new instruction_message_info_send(p); },

        [](const size_t* const p)->const instruction_block*
        { return new instruction_message_create(p); },

        [](const size_t* const p)->const instruction_block*
        { return new instruction_message_part_create(p); },

        [](const size_t* const p)->const instruction_block*
        { return new instruction_message_include_child_to_parent(p); },

        [](const size_t* const p)->const instruction_block*
        { return new instruction_task_execute(p); },

        [](const size_t* const p)->const instruction_block*
        { return new instruction_task_create(p); },

        [](const size_t* const p)->const instruction_block*
        { return new instruction_task_result(p); },

        [](const size_t* const p)->const instruction_block*
        { return new instruction_add_result_to_memory(p); },

        [](const size_t* const p)->const instruction_block*
        { return new instruction_message_delete(p); },

        [](const size_t* const p)->const instruction_block*
        { return new instruction_task_delete(p); }
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
    { return 4; }

    message_id instruction_message_send::id() const
    { return {ins[1], static_cast<process>(ins[2])}; }

    process instruction_message_send::proc() const
    { return static_cast<process>(ins[3]); }

    void instruction::add_message_sending(message_id id, process proc)
    {
        add_cmd(INSTRUCTION::MES_SEND);
        write(id);
        v.push_back(proc);
    }

    // MES_RECV
    instruction_message_recv::instruction_message_recv(const size_t* const p): instruction_block(p)
    { }

    size_t instruction_message_recv::size() const
    { return 4; }

    message_id instruction_message_recv::id() const
    { return {ins[1], static_cast<process>(ins[2])}; }

    process instruction_message_recv::proc() const
    { return static_cast<process>(ins[3]); }

    void instruction::add_message_receiving(message_id id, process proc)
    {
        add_cmd(INSTRUCTION::MES_RECV);
        write(id);
        v.push_back(proc);
    }

    // MES_INFO_SEND
    instruction_message_info_send::instruction_message_info_send(const size_t* const p): instruction_block(p)
    { }

    size_t instruction_message_info_send::size() const
    { return 4; }

    message_id instruction_message_info_send::id() const
    { return {ins[1], static_cast<process>(ins[2])}; }

    process instruction_message_info_send::proc() const
    { return static_cast<process>(ins[3]); }

    void instruction::add_message_info_sending(message_id id, process proc)
    {
        add_cmd(INSTRUCTION::MES_INFO_SEND);
        write(id);
        v.push_back(proc);
    }

    // MES_CREATE
    instruction_message_create::instruction_message_create(const size_t* const p): instruction_block(p)
    { }

    size_t instruction_message_create::size() const
    { return 5; }

    message_id instruction_message_create::id() const
    { return {ins[1], static_cast<process>(ins[2])}; }

    size_t instruction_message_create::type() const
    { return ins[3]; }

    process instruction_message_create::proc() const
    { return static_cast<process>(ins[4]); }

    void instruction::add_message_creation(message_id id, message_type type, process proc)
    {
        add_cmd(INSTRUCTION::MES_CREATE);
        write(id);
        v.push_back(type);
        v.push_back(proc);
    }

    // MES_P_CREATE
    instruction_message_part_create::instruction_message_part_create(const size_t* const p): instruction_block(p)
    { }

    size_t instruction_message_part_create::size() const
    { return 7; }

    message_id instruction_message_part_create::id() const
    { return {ins[1], static_cast<process>(ins[2])}; }

    size_t instruction_message_part_create::type() const
    { return ins[3]; }

    message_id instruction_message_part_create::source() const
    { return {ins[4], static_cast<process>(ins[5])}; }

    process instruction_message_part_create::proc() const
    { return static_cast<process>(ins[6]); }

    void instruction::add_message_part_creation(message_id id, message_type type, message_id source, process proc)
    {
        add_cmd(INSTRUCTION::MES_P_CREATE);
        write(id);
        v.push_back(type);
        write(source);
        v.push_back(proc);
    }

    // INCLUDE_MES_CHILD
    instruction_message_include_child_to_parent::instruction_message_include_child_to_parent(const size_t* const p): instruction_block(p)
    { }

    size_t instruction_message_include_child_to_parent::size() const
    { return 5; }

    message_id instruction_message_include_child_to_parent::parent() const
    { return {ins[1], static_cast<process>(ins[2])}; }

    message_id instruction_message_include_child_to_parent::child() const
    { return {ins[3], static_cast<process>(ins[4])}; }

    void instruction::add_include_child_to_parent(message_id parent, message_id child)
    {
        add_cmd(INSTRUCTION::INCLUDE_MES_CHILD);
        write(parent);
        write(child);
    }

    // TASK_EXE
    instruction_task_execute::instruction_task_execute(const size_t* const p): instruction_block(p)
    { }

    size_t instruction_task_execute::size() const
    { return 3; }

    task_id instruction_task_execute::id() const
    { return {ins[1], static_cast<process>(ins[2])}; }

    void instruction::add_task_execution(task_id id)
    {
        add_cmd(INSTRUCTION::TASK_EXE);
        write(id);
    }

    // TASK_CREATE
    instruction_task_create::instruction_task_create(const size_t* const p): instruction_block(p)
    { }

    size_t instruction_task_create::size() const
    {
        size_t n = ins[6];
        return 8 + n * 2 + ins[7 + n * 2] * 2;
    }

    task_id instruction_task_create::id() const
    { return {ins[1], static_cast<process>(ins[2])}; }

    message_id instruction_task_create::base_id() const
    { return {ins[3], static_cast<process>(ins[4])}; }

    task_type instruction_task_create::type() const
    { return ins[5]; }

    std::vector<message_id> instruction_task_create::data() const
    {
        size_t pos = 6;
        return read_vector<message_id>(pos);
    }

    std::vector<message_id> instruction_task_create::const_data() const
    {
        size_t pos = 7 + ins[6] * 2;
        return read_vector<message_id>(pos);
    }

    void instruction::add_task_creation(task_id id, message_id base_id, task_type type, std::vector<message_id> data, std::vector<message_id> c_data)
    {
        add_cmd(INSTRUCTION::TASK_CREATE);
        write(id);
        write(base_id);
        v.push_back(type);
        write_vector(data);
        write_vector(c_data);
    }

    // TASK_RES
    instruction_task_result::instruction_task_result(const size_t* const p): instruction_block(p)
    { }

    size_t instruction_task_result::size() const
    { return 3; }

    task_id instruction_task_result::id() const
    { return {ins[1], static_cast<process>(ins[2])}; }

    void instruction::add_task_result(task_id id)
    {
        add_cmd(INSTRUCTION::TASK_RES);
        write(id);
    }

    // ADD_RES_TO_MEMORY
    instruction_add_result_to_memory::instruction_add_result_to_memory(const size_t* const p): instruction_block(p)
    {
        offsets[0] = 1;
        offsets[1] = offsets[0] + ins[offsets[0]] * 2 + 1;
        offsets[2] = offsets[1] + ins[offsets[1]] * 4 + 1;
    }

    size_t instruction_add_result_to_memory::size() const
    { return offsets[2]; }

    std::vector<message_id> instruction_add_result_to_memory::added_messages_init() const
    {
        size_t pos = offsets[0];
        return read_vector<message_id>(pos);
    }

    std::vector<std::pair<message_id, message_id>> instruction_add_result_to_memory::added_messages_child() const
    {
        size_t pos = offsets[1];
        return read_vector<std::pair<message_id, message_id>>(pos);
    }

    void instruction::add_add_result_to_memory(const std::vector<message_id>& mes, const std::vector<std::pair<message_id, message_id>>& mes_c)
    {
        add_cmd(INSTRUCTION::ADD_RES_TO_MEMORY);
        write_vector(mes);
        write_vector(mes_c);
    }

    // MES_DEL
    instruction_message_delete::instruction_message_delete(const size_t* const p): instruction_block(p)
    { }

    size_t instruction_message_delete::size() const
    { return 3; }

    message_id instruction_message_delete::id() const
    { return {ins[1], static_cast<process>(ins[2])}; }

    void instruction::add_message_del(message_id id)
    {
        add_cmd(INSTRUCTION::MES_DEL);
        write(id);
    }

    // TASK_DEL
    instruction_task_delete::instruction_task_delete(const size_t* const p): instruction_block(p)
    { }

    size_t instruction_task_delete::size() const
    { return 3; }

    task_id instruction_task_delete::id() const
    { return {ins[1], static_cast<process>(ins[2])}; }

    void instruction::add_task_del(task_id id)
    {
        add_cmd(INSTRUCTION::TASK_DEL);
        write(id);
    }

}
