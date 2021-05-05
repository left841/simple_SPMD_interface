#ifndef __TASK_GRAPH_H__
#define __TASK_GRAPH_H__

#include <vector>
#include <map>
#include <set>
#include <algorithm>
#include "apl/parallel_defs.h"
#include "apl/task.h"

namespace apl
{

    struct message_id
    {
        size_t num;
        process proc;

        bool operator!=(const message_id& other) const;
        bool operator<(const message_id& other) const;
    };

    template<>
    struct simple_datatype_map<message_id>
    {
        using map = type_map
        <
            type_offset<size_t, offsetof(message_id, num)>,
            type_offset<process, offsetof(message_id, proc)>
        >;
    };

    struct perform_id
    {
        size_t num;
        process proc;

        bool operator!=(const perform_id& other) const;
        bool operator<(const perform_id& other) const;
    };

    template<>
    struct simple_datatype_map<perform_id>
    {
        using map = type_map
        <
            type_offset<size_t, offsetof(perform_id, num)>,
            type_offset<process, offsetof(perform_id, proc)>
        >;
    };

    struct task_id
    {
        message_id mi;
        perform_id pi;

        bool operator!=(const task_id& other) const;
        bool operator<(const task_id& other) const;
    };

    template<>
    struct simple_datatype_map<task_id>
    {
        using map = type_map
        <
            type_offset<size_t, offsetof(task_id, mi)>,
            type_offset<process, offsetof(task_id, pi)>
        >;
    };

    class task_graph
    {
    protected:

        size_t base_perform_id;
        size_t base_message_id;

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
        template<typename Type, typename... Args>
        void add_task(task* t, const std::vector<message*>& data);

        void add_message(message* m, message_type type, const std::vector<message*>& info);
        template<typename Type, typename... InfoTypes>
        void add_message(message* m, const std::vector<message*>& info);

        void add_dependence(task* parent, task* child);

        void del_task(task* t);
        void del_message(message* m);
        void del_dependence(task* parent, task* child);

        bool contain_task(task* t) const;
        bool contain_data(message* m) const;
        bool contain_dependence(task* parent, task* child) const;

        void clear();

        friend class memory_manager;
        friend class memory_manager2;
    };

    template<typename Type, typename... Args>
    void task_graph::add_task(task* t, const std::vector<message*>& data)
    { add_task(t, {message_init_factory::get_type<Type>(), task_factory::get_type<Type, Args...>()}, data, {}); }

    template<typename Type, typename... InfoTypes>
    void task_graph::add_message(message* m, const std::vector<message*>& info)
    { add_message(m, message_init_factory::get_type<Type, InfoTypes...>(), info); }

}

#endif // __TASK_GRAPH_H__
