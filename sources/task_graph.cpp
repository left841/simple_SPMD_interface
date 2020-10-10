#include "task_graph.h"

namespace apl
{

    bool message_id::operator!=(const message_id& other) const
    { return (num != other.num) || (proc != other.proc); }

    bool perform_id::operator!=(const perform_id& other) const
    { return (num != other.num) || (proc != other.proc); }

    bool task_id::operator!=(const task_id& other) const
    { return (mi != other.mi) || (pi != other.pi); }

    bool message_id::operator<(const message_id& other) const
    { return (num != other.num) ? (num < other.num): (proc < other.proc); }

    bool perform_id::operator<(const perform_id& other) const
    { return (num != other.num) ? (num < other.num): (proc < other.proc); }

    bool task_id::operator<(const task_id& other) const
    { return (mi != other.mi) ? (mi < other.mi): (pi < other.pi); }

    task_graph::task_graph()
    { base_message_id = base_perform_id = 0; }

    task_graph::task_graph(const task_graph& _tg)
    {
        base_message_id = _tg.base_message_id;
        base_perform_id = _tg.base_perform_id;
        t_map = _tg.t_map;
        d_map = _tg.d_map;
    }

    task_graph& task_graph::operator=(const task_graph& _tg)
    {
        if (&_tg == this)
            return *this;
        base_message_id = _tg.base_message_id;
        base_perform_id = _tg.base_perform_id;
        t_map = _tg.t_map;
        d_map = _tg.d_map;
        return *this;
    }

    void task_graph::add_task(task* t, task_type type, const std::vector<message*>& data, const std::vector<message*>& info)
    {
        for (message* i: data)
        {
            if (d_map.find(i) == d_map.end())
            {
                std::vector<message*> m_info;
                d_map.insert({i, {{base_message_id++, MPI_PROC_NULL}, MESSAGE_TYPE_UNDEFINED, 1, m_info}});
            }
            else
                ++d_map[i].ref_count;
        }
        std::set<task*> childs;
        std::set<task*> parents;
        if (d_map.find(t) == d_map.end())
            d_map.insert({t, {{base_message_id++, MPI_PROC_NULL}, type.mt, 1, info}});
        else
            ++d_map[t].ref_count;
        t_map.insert({t, {{base_perform_id++, MPI_PROC_NULL}, type.pt, data, childs, parents}});
    }

    void task_graph::add_data(message* m, message_type type, const std::vector<message*>& info)
    { d_map.insert({m, {{base_message_id++, MPI_PROC_NULL}, type, 1, info}}); }

    void task_graph::add_dependence(task* parent, task* child)
    {
        t_map[parent].childs.insert(child);
        t_map[child].parents.insert(parent);
    }

    void task_graph::del_task(task* t)
    {
        if (t_map.find(t) == t_map.end())
            return;
        std::set<task*>& tmp = t_map[t].childs;
        for (auto it = tmp.begin(); it != tmp.end(); ++it)
            t_map[(*it)].parents.erase(t);
        tmp = t_map[t].parents;
        for (auto it = tmp.begin(); it != tmp.end(); ++it)
            t_map[(*it)].childs.erase(t);
        for (message* i: t_map.find(t)->second.data)
        {
            --d_map[i].ref_count;
            if (d_map[i].ref_count < 1)
                d_map.erase(i);
        }
        --d_map[t].ref_count;
        if (d_map[t].ref_count < 1)
            d_map.erase(t);
        t_map.erase(t);
    }

    void task_graph::del_data(message* m)
    {
        if (d_map.find(m) == d_map.end())
            return;
        d_map.erase(m);
    }

    void task_graph::del_dependence(task* parent, task* child)
    {
        if (t_map.find(parent) != t_map.end())
            t_map[parent].childs.erase(child);
        if (t_map.find(child) != t_map.end())
            t_map[child].parents.erase(parent);
    }

    bool task_graph::contain_task(task* t)
    { return t_map.find(t) != t_map.end(); }

    bool task_graph::contain_data(message* m)
    { return d_map.find(m) != d_map.end(); }

    bool task_graph::contain_dependence(task* parent, task* child)
    {
        if (t_map.find(parent) == t_map.end())
            return false;
        return t_map[parent].childs.find(child) != t_map[parent].childs.end();
    }

    void task_graph::clear()
    {
        t_map.clear();
        d_map.clear();
    }

}
