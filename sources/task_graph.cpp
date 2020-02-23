#include "task_graph.h"

namespace auto_parallel
{

    task_graph::task_graph()
    { base_data_id = base_task_id = 0; }

    task_graph::task_graph(const task_graph& _tg)
    {
        base_data_id = _tg.base_data_id;
        base_task_id = _tg.base_task_id;
        t_map = _tg.t_map;
        d_map = _tg.d_map;
    }

    task_graph& task_graph::operator =(const task_graph& _tg)
    {
        if (&_tg == this)
            return *this;
        base_data_id = _tg.base_data_id;
        base_task_id = _tg.base_task_id;
        t_map = _tg.t_map;
        d_map = _tg.d_map;
        return *this;
    }

    void task_graph::add_task(task* t)
    {
        if (t == nullptr)
            throw -3;
        if (t_map.find(t) != t_map.end())
            throw -1;
        for (message* i:t->data)
        {
            if (d_map.find(i) == d_map.end())
                d_map.insert(std::make_pair(i, d_id(base_data_id++)));
            ++d_map[i].ref_count;
        }
        for (const message* j : t->c_data)
        {
            message* i = const_cast<message*>(j);
            if (d_map.find(i) == d_map.end())
                d_map.insert(std::make_pair(i, d_id(base_data_id++)));
            ++d_map[i].ref_count;
        }
        t_map.insert(std::make_pair(t, t_id(base_task_id++)));
    }

    void task_graph::add_data(message* m)
    {
        if (m == nullptr)
            throw -3;
        if (d_map.find(m) != d_map.end())
            throw -2;
        d_map.insert(std::make_pair(m, d_id(base_data_id++)));
    }

    void task_graph::add_dependence(task* parent, task* child)
    {
        if (t_map.find(parent) == t_map.end())
            add_task(parent);
        if (t_map.find(child) == t_map.end())
            add_task(child);
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
        for (message* i:t->data)
        {
            if (d_map.find(i) == d_map.end())
                throw -4;
            --d_map[i].ref_count;
            if (d_map[i].ref_count < 1)
                d_map.erase(i);
        }
        for (const message* j : t->c_data)
        {
            message* i = const_cast<message*>(j);
            if (d_map.find(i) == d_map.end())
                throw - 4;
            --d_map[i].ref_count;
            if (d_map[i].ref_count < 1)
                d_map.erase(i);
        }
        t_map.erase(t);
    }

    void task_graph::del_data(message* m)
    {
        if (d_map.find(m) == d_map.end())
            return;
        if (d_map[m].ref_count != 0)
            throw -5;
        d_map.erase(m);
    }

    void task_graph::del_dependence(task* parent, task* child)
    {
        if (t_map.find(parent) != t_map.end())
            t_map[parent].childs.erase(child);
        if (t_map.find(child) != t_map.end())
            t_map[child].parents.erase(parent);
    }

    void task_graph::change_task(task* old_t, task* new_t)
    {
        if (new_t == nullptr)
            throw -3;
        if (t_map.find(old_t) == t_map.end())
            throw -6;
        if (t_map.find(new_t) != t_map.end())
            throw -7;
        t_id tmp(t_map[old_t].id);
        tmp.childs = t_map[old_t].childs;
        tmp.parents = t_map[old_t].parents;
        t_map.erase(old_t);
        t_map.insert(std::make_pair(new_t, tmp));
        for (auto it = tmp.childs.begin(); it != tmp.childs.end(); ++it)
        {
            t_map[(*it)].parents.erase(old_t);
            t_map[(*it)].parents.insert(new_t);
        }
        for (auto it = tmp.parents.begin(); it != tmp.parents.end(); ++it)
        {
            t_map[(*it)].childs.erase(old_t);
            t_map[(*it)].childs.insert(new_t);
        }
        for (message* i:old_t->data)
        {
            if (d_map.find(i) == d_map.end())
                throw -4;
            --d_map[i].ref_count;
            if (d_map[i].ref_count < 1)
                d_map.erase(i);
        }
        for (message* i:new_t->data)
        {
            if (d_map.find(i) == d_map.end())
                d_map.insert(std::make_pair(i, d_id(base_data_id++)));
            ++d_map[i].ref_count;
        }
        for (const message* j : old_t->c_data)
        {
            message* i = const_cast<message*>(j);
            if (d_map.find(i) == d_map.end())
                throw - 4;
            --d_map[i].ref_count;
            if (d_map[i].ref_count < 1)
                d_map.erase(i);
        }
        for (const message* j : new_t->c_data)
        {
            message* i = const_cast<message*>(j);
            if (d_map.find(i) == d_map.end())
                d_map.insert(std::make_pair(i, d_id(base_data_id++)));
            ++d_map[i].ref_count;
        }
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

    void task_graph::add_task(task& t)
    { add_task(&t); }

    void task_graph::add_data(message& m)
    { add_data(&m); }

    void task_graph::add_dependence(task& parent, task& child)
    { add_dependence(&parent, &child); }

    void task_graph::del_task(task& t)
    { del_task(&t); }

    void task_graph::del_data(message& m)
    { del_data(&m); }

    void task_graph::del_dependence(task& parent, task& child)
    { del_dependence(&parent, &child); }

    void task_graph::change_task(task& old_t, task& new_t)
    { change_task(&old_t, &new_t); }

    bool task_graph::contain_task(task& t)
    { return contain_task(&t); }

    bool task_graph::contain_data(message& m)
    { return contain_data(&m); }

    bool task_graph::contain_dependence(task& parent, task& child)
    { return contain_dependence(&parent, &child); }

    void task_graph::clear()
    {
        t_map.clear();
        d_map.clear();
    }

}
