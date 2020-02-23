#include "parallelizer.h"

namespace auto_parallel
{

    parallelizer::instruction::instruction() : sendable()
    {
        previous = cmd::UNDEFINED;
        prev_pos = -1;
    }

    parallelizer::instruction::~instruction()
    { }

    void parallelizer::instruction::send(const sender& se)
    {
        se.send(v.data(), v.size(), MPI_INT);
    }

    void parallelizer::instruction::recv(const receiver& re)
    {
        v.resize(re.probe(MPI_INT));
        re.recv(v.data(), v.size(), MPI_INT);
    }

    int& parallelizer::instruction::operator[](size_t n)
    {
        return v[n];
    }

    const int& parallelizer::instruction::operator[](size_t n) const
    {
        return v[n];
    }

    size_t parallelizer::instruction::size()
    {
        return v.size();
    }

    void parallelizer::instruction::clear()
    {
        v.clear();
        previous = cmd::UNDEFINED;
    }

    void parallelizer::instruction::add_cmd(cmd id)
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

    void parallelizer::instruction::add_end()
    {
        add_cmd(cmd::END);
    }

    void parallelizer::instruction::add_message_sending(int id)
    {
        add_cmd(cmd::MES_SEND);
        v.push_back(id);
    }

    void parallelizer::instruction::add_message_receiving(int id)
    {
        add_cmd(cmd::MES_RECV);
        v.push_back(id);
    }

    void parallelizer::instruction::add_message_creation(int id, int type)
    {
        add_cmd(cmd::MES_CREATE);
        v.push_back(id);
        v.push_back(type);
    }

    void parallelizer::instruction::add_message_part_creation(int id, int type, int source)
    {
        add_cmd(cmd::MES_P_CREATE);
        v.push_back(id);
        v.push_back(type);
        v.push_back(source);
    }

    void parallelizer::instruction::add_task_execution(int id)
    {
        add_cmd(cmd::TASK_EXE);
        v.push_back(id);
    }

    void parallelizer::instruction::add_task_creation(int id, int type, std::vector<int> data, std::vector<int> c_data)
    {
        add_cmd(cmd::TASK_CREATE);
        v.push_back(id);
        v.push_back(type);
        v.push_back(data.size());
        for (int i : data)
            v.push_back(i);
        v.push_back(c_data.size());
        for (int i : c_data)
            v.push_back(i);
    }

    void parallelizer::instruction::add_task_result(int id, task_environment& env)
    {
        add_cmd(cmd::TASK_RES);
        std::vector<task_environment::message_data>& md = env.get_c_messages();
        std::vector<task_environment::message_part_data>& mpd = env.get_c_parts();
        std::vector<task_environment::task_data>& td = env.get_c_tasks();

        v.push_back(id);
        v.push_back(md.size());
        for (int i = 0; i < md.size(); ++i)
            v.push_back(md[i].type);
        v.push_back(mpd.size());
        for (int i = 0; i < mpd.size(); ++i)
        {
            v.push_back(mpd[i].type);
            v.push_back(mpd[i].sourse.id);
            v.push_back(static_cast<int>(mpd[i].sourse.ms));
        }
        v.push_back(td.size());
        for (int i = 0; i < td.size(); ++i)
        {
            v.push_back(td[i].type);
            v.push_back(td[i].ti->data.size());
            for (task_environment::mes_id j : td[i].ti->data)
            {
                v.push_back(j.id);
                v.push_back(static_cast<int>(j.ms));
            }
            v.push_back(td[i].ti->c_data.size());
            for (task_environment::mes_id j : td[i].ti->c_data)
            {
                v.push_back(j.id);
                v.push_back(static_cast<int>(j.ms));
            }
        }
    }

}
