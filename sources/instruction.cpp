#include "instruction.h"
//#include <iostream>
//using std::cout;
//using std::endl;

namespace auto_parallel
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

    void instruction::send(const sender& se)
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
        { return new instruction_task_result(p); }
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
    {
        offsets[0] = 2;
        offsets[1] = 3 + ins[2];
        offsets[2] = 4 + ins[2] + ins[3 + ins[2]] * 3;
        size_t m = offsets[2] + 1;
        for (size_t i = 0; i < ins[offsets[2]]; ++i)
        {
            m += 1;
            m += ins[m] * 2 + 1;
            m += ins[m] * 2 + 1;
        }
        offsets[3] = m;
        ++m;
        for (size_t i = 0; i < ins[offsets[3]]; ++i)
        {
            m += 1;
            m += ins[m] * 2 + 1;
            m += ins[m] * 2 + 1;
        }
        offsets[4] = m;
        offsets[5] = ins[m] * 4 + m + 1;
    }

    size_t instruction_task_result::size() const
    { return offsets[5]; }

    task_id instruction_task_result::id() const
    { return static_cast<task_id>(ins[1]); }

    std::vector<message_type> instruction_task_result::created_messages() const
    {
        std::vector<message_type> v(ins[offsets[0]]);
        for (size_t i = 0; i < v.size(); ++i)
            v[i] = ins[3 + i];
        return v;
    }

    std::vector<std::pair<message_type, local_message_id>> instruction_task_result::created_parts() const
    {
        size_t n = offsets[1] + 1;
        std::vector<std::pair<message_type, local_message_id>> v(ins[offsets[1]]);
        for (size_t i = 0; i < v.size(); ++i)
        {
            v[i] = {static_cast<message_type>(ins[n]), {ins[n + 1], static_cast<MESSAGE_SOURCE>(ins[n + 2])}};
            n += 3;
        }
        return v;
    }

    std::vector<task_data> instruction_task_result::created_child_tasks() const
    {
        size_t n = offsets[2];
        std::vector<task_data> v = read_vector<task_data>(n, [this](size_t& n)->task_data
        {
            task_data td {static_cast<task_type>(this->ins[n++]), {}, {}};
            td.data = read_vector<local_message_id>(n, [this](size_t& n)->local_message_id
            {
                local_message_id id;
                id.id = this->ins[n++];
                id.ms = static_cast<MESSAGE_SOURCE>(this->ins[n++]);
                return id;
            });
            td.c_data = read_vector<local_message_id>(n, [this](size_t& n)->local_message_id
            {
                local_message_id id;
                id.id = this->ins[n++];
                id.ms = static_cast<MESSAGE_SOURCE>(this->ins[n++]);
                return id;
            });
            return td;
        });
        return v;
    }

    std::vector<task_data> instruction_task_result::created_tasks() const
    {
        size_t n = offsets[3];
        std::vector<task_data> v = read_vector<task_data>(n, [this](size_t& n)->task_data
            {
                task_data td{static_cast<task_type>(this->ins[n++]), {}, {}};
                td.data = read_vector<local_message_id>(n, [this](size_t& n)->local_message_id
                    {
                        local_message_id id;
                        id.id = this->ins[n++];
                        id.ms = static_cast<MESSAGE_SOURCE>(this->ins[n++]);
                        return id;
                    });
                td.c_data = read_vector<local_message_id>(n, [this](size_t& n)->local_message_id
                    {
                        local_message_id id;
                        id.id = this->ins[n++];
                        id.ms = static_cast<MESSAGE_SOURCE>(this->ins[n++]);
                        return id;
                    });
                return td;
            });
        return v;
    }

    std::vector<task_dependence> instruction_task_result::created_task_dependences() const
    {
        size_t n = offsets[4];
        std::vector<task_dependence> v = read_vector<task_dependence>(n, [this](size_t& n)->task_dependence
        {
            task_dependence dp {ins[n], static_cast<TASK_SOURCE>(ins[n + 1]), ins[n + 2], static_cast<TASK_SOURCE>(ins[n + 3])};
            n += 4;
            return dp;
        });
        return v;
    }

    void instruction::add_task_result(task_id id, task_environment& env)
    {
        add_cmd(INSTRUCTION::TASK_RES);
        std::vector<message_data>& md = env.created_messages();
        std::vector<message_part_data>& mpd = env.created_parts();
        std::vector<task_data>& td = env.created_child_tasks();
        std::vector<task_data>& cre_t = env.created_tasks();
        std::vector<task_dependence>& dep = env.created_dependences();

        v.push_back(id);
        v.push_back(md.size());
        for (size_t i = 0; i < md.size(); ++i)
            v.push_back(md[i].type);
        v.push_back(mpd.size());
        for (size_t i = 0; i < mpd.size(); ++i)
        {
            v.push_back(mpd[i].type);
            v.push_back(mpd[i].sourse.id);
            v.push_back(static_cast<size_t>(mpd[i].sourse.ms));
        }
        v.push_back(td.size());
        for (size_t i = 0; i < td.size(); ++i)
        {
            v.push_back(td[i].type);
            v.push_back(td[i].data.size());
            for (local_message_id j: td[i].data)
            {
                v.push_back(j.id);
                v.push_back(static_cast<size_t>(j.ms));
            }
            v.push_back(td[i].c_data.size());
            for (local_message_id j: td[i].c_data)
            {
                v.push_back(j.id);
                v.push_back(static_cast<size_t>(j.ms));
            }
        }
        v.push_back(cre_t.size());
        for (task_data& i: cre_t)
        {
            v.push_back(i.type);
            v.push_back(i.data.size());
            for (local_message_id j: i.data)
            {
                v.push_back(j.id);
                v.push_back(static_cast<size_t>(j.ms));
            }
            v.push_back(i.c_data.size());
            for (local_message_id j: i.c_data)
            {
                v.push_back(j.id);
                v.push_back(static_cast<size_t>(j.ms));
            }
        }
        v.push_back(dep.size());
        for (task_dependence& i: dep)
        {
            v.push_back(i.parent.id);
            v.push_back(static_cast<size_t>(i.parent.src));
            v.push_back(i.child.id);
            v.push_back(static_cast<size_t>(i.child.src));
        }
    }

}
