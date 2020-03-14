#include "instruction.h"

namespace auto_parallel
{

    instruction::instruction() : sendable(), previous(cmd::UNDEFINED), prev_pos()
    { }

    instruction::~instruction()
    { }

    void instruction::send(const sender& se)
    {
        se.send(v.data(), static_cast<int>(v.size()), MPI_INT);
    }

    void instruction::recv(const receiver& re)
    {
        v.resize(re.probe(MPI_INT));
        re.recv(v.data(), static_cast<int>(v.size()), MPI_INT);
    }

    int& instruction::operator[](size_t n)
    {
        return v[n];
    }

    const int& instruction::operator[](size_t n) const
    {
        return v[n];
    }

    size_t instruction::size()
    {
        return v.size();
    }

    void instruction::clear()
    {
        v.clear();
        previous = cmd::UNDEFINED;
    }

    void instruction::add_cmd(cmd id)
    {
        if (id == previous)
            ++v[prev_pos + 1];
        else
        {
            previous = id;
            prev_pos = v.size();
            v.push_back(static_cast<int>(id));
            v.push_back(1);
        }
    }

    void instruction::add_end()
    {
        add_cmd(cmd::END);
    }

    void instruction::add_message_sending(int id)
    {
        add_cmd(cmd::MES_SEND);
        v.push_back(id);
    }

    void instruction::add_message_receiving(int id)
    {
        add_cmd(cmd::MES_RECV);
        v.push_back(id);
    }

    void instruction::add_message_creation(int id, int type)
    {
        add_cmd(cmd::MES_CREATE);
        v.push_back(id);
        v.push_back(type);
    }

    void instruction::add_message_part_creation(int id, int type, int source)
    {
        add_cmd(cmd::MES_P_CREATE);
        v.push_back(id);
        v.push_back(type);
        v.push_back(source);
    }

    void instruction::add_task_execution(int id)
    {
        add_cmd(cmd::TASK_EXE);
        v.push_back(id);
    }

    void instruction::add_task_creation(size_t id, size_t type, std::vector<message_id> data, std::vector<message_id> c_data)
    {
        add_cmd(cmd::TASK_CREATE);
        v.push_back(id);
        v.push_back(type);
        v.push_back(static_cast<int>(data.size()));
        for (message_id i : data)
            v.push_back(i);
        v.push_back(static_cast<int>(c_data.size()));
        for (message_id i : c_data)
            v.push_back(i);
    }

    void instruction::add_task_result(int id, task_environment& env)
    {
        add_cmd(cmd::TASK_RES);
        std::vector<task_environment::message_data>& md = env.get_c_messages();
        std::vector<task_environment::message_part_data>& mpd = env.get_c_parts();
        std::vector<task_environment::task_data>& td = env.get_c_tasks();

        v.push_back(id);
        v.push_back(static_cast<int>(md.size()));
        for (int i = 0; i < md.size(); ++i)
            v.push_back(md[i].type);
        v.push_back(static_cast<int>(mpd.size()));
        for (int i = 0; i < mpd.size(); ++i)
        {
            v.push_back(mpd[i].type);
            v.push_back(mpd[i].sourse.id);
            v.push_back(static_cast<int>(mpd[i].sourse.ms));
        }
        v.push_back(static_cast<int>(td.size()));
        for (int i = 0; i < td.size(); ++i)
        {
            v.push_back(td[i].type);
            v.push_back(static_cast<int>(td[i].ti->data.size()));
            for (task_environment::mes_id j : td[i].ti->data)
            {
                v.push_back(j.id);
                v.push_back(static_cast<int>(j.ms));
            }
            v.push_back(static_cast<int>(td[i].ti->c_data.size()));
            for (task_environment::mes_id j : td[i].ti->c_data)
            {
                v.push_back(j.id);
                v.push_back(static_cast<int>(j.ms));
            }
        }
    }

}
