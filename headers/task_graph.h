#ifndef __TASK_GRAPH_H__
#define __TASK_GRAPH_H__

#include "basic_task.h"
#include <vector>
#include <map>
#include <set>
#include <algorithm>

namespace auto_parallel
{

    class task_graph
    {
    protected:

        int base_task_id;
        int base_data_id;

        struct d_id
        {
            const int id;
            int ref_count;
            d_id(const int nid = 0): id(nid)
            { ref_count = 0; }
        };

        struct t_id
        {
            const int id;
            std::set<task*> childs;
            std::set<task*> parents;
            t_id(const int nid = 0): id(nid)
            { }
        };

        std::map<task*, t_id> t_map;
        std::map<message*, d_id> d_map;

    public:

        task_graph();
        task_graph(const task_graph& _tg);
        task_graph& operator =(const task_graph& _tg);

        void add_task(task* t);
        void add_task(task& t);
        void add_data(message* m);
        void add_data(message& m);
        void add_dependence(task* parent, task* child);
        void add_dependence(task& parent, task& child);

        void del_task(task* t);
        void del_task(task& t);
        void del_data(message* m);
        void del_data(message& m);
        void del_dependence(task* parent, task* child);
        void del_dependence(task& parent, task& child);

        void change_task(task* old_t, task* new_t);
        void change_task(task& old_t, task& new_t);

        bool contain_task(task* t);
        bool contain_task(task& t);
        bool contain_data(message* m);
        bool contain_data(message& m);
        bool contain_dependence(task* parent, task* child);
        bool contain_dependence(task& parent, task& child);

        void clear();

        friend class parallelizer;
    };

}

#endif // __TASK_GRAPH_H__
