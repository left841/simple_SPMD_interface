#ifndef __TASK_GRAPH_H__
#define __TASK_GRAPH_H__

#include "parallel_defs.h"
#include "basic_task.h"
#include <vector>
#include <map>
#include <set>
#include <algorithm>

namespace apl
{

    class task_graph
    {
    protected:

        perform_id base_perform_id;
        message_id base_message_id;

        struct d_id
        {
            message_id id;
            message_type type;
            size_t ref_count;
            std::vector<message*> info;
        };

        struct t_id
        {
            perform_id id;
            perform_type type;
            std::vector<message*> data;
            std::set<task*> childs;
            std::set<task*> parents;
        };

        std::map<task*, t_id> t_map;
        std::map<message*, d_id> d_map;

    public:

        task_graph();
        task_graph(const task_graph& _tg);
        task_graph& operator=(const task_graph& _tg);

        void add_task(task* t, task_type type, const std::vector<message*>& data, const std::vector<message*>& info);
        //void add_task(task& t);
        void add_data(message* m, message_type type, const std::vector<message*>& info);
        //void add_data(message& m);
        void add_dependence(task* parent, task* child);
        //void add_dependence(task& parent, task& child);

        void del_task(task* t);
        //void del_task(task& t);
        void del_data(message* m);
        //void del_data(message& m);
        void del_dependence(task* parent, task* child);
        //void del_dependence(task& parent, task& child);

        bool contain_task(task* t);
        //bool contain_task(task& t);
        bool contain_data(message* m);
        //bool contain_data(message& m);
        bool contain_dependence(task* parent, task* child);
        //bool contain_dependence(task& parent, task& child);

        void clear();

        friend class parallelizer;
        friend class memory_manager;
    };

}

#endif // __TASK_GRAPH_H__
