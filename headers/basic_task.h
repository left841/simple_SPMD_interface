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
        std::vector<sendable*> ii;
    };

    struct message_init_add_data
    {
        message_type type;
        std::vector<sendable*> ii;
        message* mes;
    };

    struct message_child_data
    {
        message_type type;
        local_message_id sourse;
        std::vector<sendable*> pi;
    };

    struct message_child_add_data
    {
        message_type type;
        local_message_id sourse;
        std::vector<sendable*> pi;
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

        size_t proc_count;
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

        local_message_id create_message_init(message_type type, const std::vector<sendable*>& info);
        local_message_id create_message_child(message_type type, local_message_id source, const std::vector<sendable*>& info);

        local_message_id add_message_init(message_type type, message* m, const std::vector<sendable*>& info);
        local_message_id add_message_child(message_type type, message* m, local_message_id source, const std::vector<sendable*>& info);

        local_task_id create_task(task_type type, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data);
        local_task_id create_child_task(task_type type, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data);

        local_task_id add_task(task_type type, task* t, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data);
        local_task_id add_child_task(task_type type, task* t, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data);

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

        local_message_id arg_id(size_t n);
        local_message_id const_arg_id(size_t n);
        task_data get_this_task_data();
        local_task_id get_this_task_id();

        void set_proc_count(size_t sz);
        size_t working_processes();

        void send(const sender& se) const;
        void recv(const receiver& re);

    };

    class task
    {
    private:

        std::vector<message*> data;
        std::vector<const message*> c_data;
        task_environment* env;

        void set_environment(task_environment* e);

    protected:

        template<class Type, class... InfoTypes>
        local_message_id create_message_init(InfoTypes*... info);
        template<class Type, class ParentType = Type, class... InfoTypes>
        local_message_id create_message_child(local_message_id source, InfoTypes*... info);

        template<class Type, class... InfoTypes>
        local_message_id add_message_init(Type* m, InfoTypes*... info);
        template<class Type, class ParentType = Type, class... InfoTypes>
        local_message_id add_message_child(Type* m, local_message_id source, InfoTypes*... info);

        template<class Type>
        local_task_id create_task(const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data);
        template<class Type>
        local_task_id create_child_task(const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data);

        template<class Type>
        local_task_id add_task(Type* t, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data);
        template<class Type>
        local_task_id add_child_task(Type* t, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data);

        void add_dependence(local_task_id parent, local_task_id child);

        local_message_id arg_id(size_t n);
        local_message_id const_arg_id(size_t n);
        task_data this_task_data();
        local_task_id this_task_id();

        size_t working_processes();

    public:

        task();
        task(const std::vector<message*>& mes_v);
        task(const std::vector<message*>& mes_v, const std::vector<const message*>& c_mes_v);
        virtual ~task();

        virtual void perform() = 0;

        void put_arg(message* mes);
        void put_const_arg(const message* mes);

        message& arg(size_t id);
        const message& const_arg(size_t id);

        friend class task_graph;
        friend class parallelizer;
        friend class memory_manager;
    };

    template<class Type, class... InfoTypes>
    local_message_id task::create_message_init(InfoTypes*... info)
    {
        std::vector<sendable*> v;
        tuple_processers<sizeof...(InfoTypes), InfoTypes...>::create_vector_from_pointers(v, std::make_tuple(info...));
        return env->create_message_init(message_init_factory::get_type<Type, InfoTypes...>(), v);
    }

    template<class Type, class ParentType, class... InfoTypes>
    local_message_id task::create_message_child(local_message_id source, InfoTypes*... info)
    {
        std::vector<sendable*> v;
        tuple_processers<sizeof...(InfoTypes), InfoTypes...>::create_vector_from_pointers(v, std::make_tuple(info...));
        return env->create_message_child(message_child_factory::get_type<Type, ParentType, InfoTypes...>(), source, v);
    }

    template<class Type, class... InfoTypes>
    local_message_id task::add_message_init(Type* m, InfoTypes*... info)
    {
        std::vector<sendable*> v;
        tuple_processers<sizeof...(InfoTypes), InfoTypes...>::create_vector_from_pointers(v, std::make_tuple(info...));
        return env->add_message_init(message_init_factory::get_type<Type, InfoTypes...>(), m, v);
    }

    template<class Type, class ParentType, class... InfoTypes>
    local_message_id task::add_message_child(Type* m, local_message_id source, InfoTypes*... info)
    {
        std::vector<sendable*> v;
        tuple_processers<sizeof...(InfoTypes), InfoTypes...>::create_vector_from_pointers(v, std::make_tuple(info...));
        return env->add_message_child(message_child_factory::get_type<Type, ParentType, InfoTypes...>(), source, v);
    }

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
    local_task_id task::create_task(const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data)
    { return env->create_task(task_factory::get_type<Type>(), data, const_data); }

    template<class Type>
    local_task_id task::create_child_task(const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data)
    { return env->create_child_task(task_factory::get_type<Type>(), data, const_data); }

    template<class Type>
    local_task_id task::add_task(Type* t, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data)
    { return env->add_task(task_factory::get_type<Type>(), t, data, const_data); }

    template<class Type>
    local_task_id task::add_child_task(Type* t, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data)
    { return env->add_child_task(task_factory::get_type<Type>(), t, data, const_data); }

}

#endif // __BASIC_TASK_H__
