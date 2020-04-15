#ifndef __BASIC_TASK_H__
#define __BASIC_TASK_H__

#include <vector>
#include "parallel_defs.h"
#include "message.h"
#include "message_factory.h"

namespace apl
{

    class task;

    enum class MESSAGE_SOURCE: size_t
    {
        TASK_ARG, TASK_ARG_C, REFERENCE,
        SIMPLE, COPY, INIT, CHILD, PART,
        SIMPLE_A, COPY_A, INIT_A, CHILD_A, PART_A
    };

    enum class TASK_SOURCE: size_t
    {
        GLOBAL, SIMPLE, SIMPLE_A, REFERENCE,
        SIMPLE_C, SIMPLE_AC
    };

    enum class TASK_FACTORY_TYPE: size_t
    {
        UNDEFINED, SIMPLE
    };

    struct local_task_id
    {
        size_t id;
        TASK_SOURCE src;
    };

    struct local_message_id
    {
        size_t id;
        MESSAGE_SOURCE src;
    };

    struct task_data
    {
        task_type type;
        std::vector<local_message_id> data, c_data;
    };

    struct task_add_data
    {
        task_type type;
        std::vector<local_message_id> data, c_data;
        task* t;
    };

    struct message_init_data
    {
        message_type type;
        sendable* ii;
    };

    struct message_init_add_data
    {
        message_type type;
        sendable* ii;
        message* mes;
    };

    struct message_child_data
    {
        message_type type;
        local_message_id sourse;
        sendable* pi;
    };

    struct message_child_add_data
    {
        message_type type;
        local_message_id sourse;
        sendable* pi;
        message* mes;
    };

    struct task_dependence
    {
        local_task_id parent;
        local_task_id child;
    };

    class task_environment: public sendable
    {
    private:

        task_data this_task;
        local_task_id this_task_id;

        std::vector<local_message_id> created_messages_v;
        std::vector<message_init_data> messages_init_v;
        std::vector<message_init_add_data> messages_init_add_v;
        std::vector<message_child_data> messages_childs_v;
        std::vector<message_child_add_data> messages_childs_add_v;

        std::vector<local_task_id> created_tasks_v;
        std::vector<task_data> tasks_v;
        std::vector<task_add_data> tasks_add_v;
        std::vector<task_data> tasks_child_v;
        std::vector<task_add_data> tasks_child_add_v;

        std::vector<task_dependence> dependence_v;

    public:

        task_environment(task_data& td, task_id id);
        task_environment(task_data&& td, task_id id);

        template<class Type>
        local_task_id create_task(const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data);
        template<class Type>
        local_task_id create_child_task(const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data);

        template<class Type>
        local_task_id add_task(task* t, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data);
        template<class Type>
        local_task_id add_child_task(task* t, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data);

        template<class Type, class InfoType>
        local_message_id create_message_init(sendable* info);
        template<class Type, class ParentType, class InfoType>
        local_message_id create_message_child(local_message_id source, sendable* info);

        template<class Type, class InfoType>
        local_message_id add_message_init(message* m, sendable* info);
        template<class Type, class ParentType, class InfoType>
        local_message_id add_message_child(message* m, local_message_id source, sendable* info);

        void add_dependence(local_task_id parent, local_task_id child);

        std::vector<local_task_id>& result_task_ids();
        std::vector<task_data>& created_tasks_simple();
        std::vector<task_data>& created_child_tasks();
        std::vector<task_add_data>& added_tasks();
        std::vector<task_add_data>& added_child_tasks();

        std::vector<local_message_id>& result_message_ids();
        std::vector<message_init_data>& created_messages_init();
        std::vector<message_child_data>& created_messages_child();
        std::vector<message_init_add_data>& added_messages_init();
        std::vector<message_child_add_data>& added_messages_child();

        std::vector<task_dependence>& created_dependences();

        local_message_id get_arg_id(size_t n);
        local_message_id get_c_arg_id(size_t n);
        task_data get_this_task_data();
        local_task_id get_this_task_id();

        void send(const sender& se);
        void recv(const receiver& re);

    };

    template<class Type, class InfoType>
    local_message_id task_environment::create_message_init(sendable* info)
    {
        messages_init_v.push_back({message_init_factory::get_type<Type, InfoType>(), info});
        created_messages_v.push_back({messages_init_v.size() - 1, MESSAGE_SOURCE::INIT});
        return created_messages_v.back();
    }

    template<class Type, class ParentType, class InfoType>
    local_message_id task_environment::create_message_child(local_message_id source, sendable* info)
    {
        messages_childs_v.push_back({message_child_factory::get_type<Type, ParentType, InfoType>(), source, info});
        created_messages_v.push_back({messages_childs_v.size() - 1, MESSAGE_SOURCE::CHILD});
        return created_messages_v.back();
    }

    template<class Type, class InfoType>
    local_message_id task_environment::add_message_init(message* m, sendable* info)
    {
        messages_init_add_v.push_back({message_init_factory::get_type<Type, InfoType>(), info, m});
        created_messages_v.push_back({messages_init_add_v.size() - 1, MESSAGE_SOURCE::INIT_A});
        return created_messages_v.back();
    }
    template<class Type, class ParentType, class InfoType>
    local_message_id task_environment::add_message_child(message* m, local_message_id source, sendable* info)
    {
        messages_childs_add_v.push_back({message_child_factory::get_type<Type, ParentType, InfoType>(), source, info, m});
        created_messages_v.push_back({messages_childs_add_v.size() - 1, MESSAGE_SOURCE::CHILD_A});
        return created_messages_v.back();
    }

    class task
    {
    protected:

        std::vector<message*> data;
        std::vector<const message*> c_data;

    public:

        task();
        task(const std::vector<message*>& mes_v);
        task(const std::vector<message*>& mes_v, const std::vector<const message*>& c_mes_v);
        virtual ~task();

        virtual void perform(task_environment& env) = 0;

        void put_a(message* mes);
        void put_c(const message* mes);

        message& get_a(size_t id);
        const message& get_c(size_t id);

        friend class task_graph;
        friend class parallelizer;
        friend class memory_manager;
    };

    class task_factory
    {
    private:

        class creator_base
        {
        public:

            creator_base();
            virtual ~creator_base();

            virtual task* get_task(std::vector<message*>& data, std::vector<const message*>& c_data) = 0;

        };

        template<typename Type>
        struct id
        {
            task_type value;
            id();
        };

        template<typename Type>
        class creator: public creator_base
        {
        private:

            static id<Type> my_type;

        public:

            creator();
            ~creator();

            task* get_task(std::vector<message*>& data, std::vector<const message*>& c_data);
            static task_type get_type();

            friend class task_factory;
        };

        task_factory() = delete;
        static std::vector<creator_base*>& task_vec();

        template<typename Type>
        static task_type add();

    public:

        static task* get(task_type id, std::vector<message*>& data, std::vector<const message*>& c_data);

        template<typename Type>
        static task_type get_type();

    };

    template<typename Type>
    task_factory::id<Type>::id(): value(task_factory::add<Type>())
    { }

    template<typename Type>
    task_factory::creator<Type>::creator()
    { }

    template<typename Type>
    task_factory::creator<Type>::~creator()
    { }

    template<typename Type>
    task* task_factory::creator<Type>::get_task(std::vector<message*>& data, std::vector<const message*>& c_data)
    { return new Type(data, c_data); }

    template<typename Type>
    task_factory::id<Type> task_factory::creator<Type>::my_type;

    template<typename Type>
    task_type task_factory::creator<Type>::get_type()
    { return my_type.value; }

    template<typename Type>
    task_type task_factory::add()
    {
        task_vec().push_back(new creator<Type>);
        return task_vec().size() - 1;
    }

    template<typename Type>
    task_type task_factory::get_type()
    { return creator<Type>::get_type(); }

    template<class Type>
    local_task_id task_environment::create_task(const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data)
    {
        tasks_v.push_back({task_factory::get_type<Type>(), data, const_data});
        created_tasks_v.push_back({tasks_v.size() - 1, TASK_SOURCE::SIMPLE});
        return created_tasks_v.back();
    }

    template<class Type>
    local_task_id task_environment::create_child_task(const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data)
    {
        tasks_child_v.push_back({task_factory::get_type<Type>(), data, const_data});
        created_tasks_v.push_back({tasks_child_v.size() - 1, TASK_SOURCE::SIMPLE_C});
        return created_tasks_v.back();
    }

    template<class Type>
    local_task_id task_environment::add_task(task* t, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data)
    {
        tasks_add_v.push_back({task_factory::get_type<Type>(), data, const_data, t});
        created_tasks_v.push_back({tasks_add_v.size() - 1, TASK_SOURCE::SIMPLE_A});
        return created_tasks_v.back();
    }

    template<class Type>
    local_task_id task_environment::add_child_task(task* t, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data)
    {
        tasks_child_add_v.push_back({task_factory::get_type<Type>(), data, const_data, t});
        created_tasks_v.push_back({tasks_child_add_v.size() - 1, TASK_SOURCE::SIMPLE_AC});
        return created_tasks_v.back();
    }

}

#endif // __BASIC_TASK_H__
