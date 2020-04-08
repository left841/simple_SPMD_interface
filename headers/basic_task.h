#ifndef __BASIC_TASK_H__
#define __BASIC_TASK_H__

#include <vector>
#include "parallel_defs.h"
#include "message.h"
#include "message_factory.h"

namespace apl
{

    enum class MESSAGE_SOURCE: size_t
    {
        TASK_ARG, TASK_ARG_C,
        CREATED, PART
    };

    enum class TASK_SOURCE: size_t
    {
        GLOBAL, CREATED, CHILD
    };

    struct local_task_id
    {
        size_t id;
        TASK_SOURCE src;
    };

    struct local_message_id
    {
        size_t id;
        MESSAGE_SOURCE ms;
    };

    struct task_data
    {
        task_type type;
        std::vector<local_message_id> data, c_data;
    };

    struct message_data
    {
        message_type type;
        sendable* iib;
    };

    struct message_part_data
    {
        message_type type;
        local_message_id sourse;
        sendable* pib;
    };

    struct task_dependence
    {
        local_task_id parent;
        local_task_id child;
    };

    class task_result
    {
    private:

        std::vector<task_data> created_tasks_v;
        std::vector<message_data> created_messages_v;
        std::vector<message_part_data> created_parts_v;

    public:

        task_result();

        std::vector<task_data>& created_tasks();
        std::vector<message_data>& created_messages();
        std::vector<message_part_data>& created_parts();
    };

    class task_environment
    {
    private:

        task_data this_task;
        local_task_id this_task_id;
        task_result res;
        std::vector<task_data> created_tasks_v;
        std::vector<task_dependence> dependence_v;

    public:

        task_environment(task_data& td, task_id id);
        task_environment(task_data&& td, task_id id);

        template<class Type>
        local_task_id create_task(const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data);
        template<class Type>
        local_task_id create_child_task(const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data);
        template<class Type, class InfoType>
        local_message_id create_message_init(sendable* info);
        template<class Type, class ParentType, class InfoType>
        local_message_id create_message_child(local_message_id source, sendable* info);

        void add_dependence(local_task_id parent, local_task_id child);

        task_result& get_result();
        std::vector<task_data>& created_tasks();
        std::vector<task_data>& created_child_tasks();
        std::vector<message_data>& created_messages();
        std::vector<message_part_data>& created_parts();
        std::vector<task_dependence>& created_dependences();

        local_message_id get_arg_id(size_t n);
        local_message_id get_c_arg_id(size_t n);
        task_data get_this_task_data();
        local_task_id get_this_task_id();

    };

    template<class Type, class InfoType>
    local_message_id task_environment::create_message_init(sendable* info)
    {
        res.created_messages().push_back({message_init_factory::get_type<Type, InfoType>(), info});
        return {created_messages().size() - 1, MESSAGE_SOURCE::CREATED};
    }

    template<class Type, class ParentType, class InfoType>
    local_message_id task_environment::create_message_child(local_message_id source, sendable* info)
    {
        res.created_parts().push_back({message_child_factory::get_type<Type, ParentType, InfoType>(), source, info});
        return {res.created_parts().size() - 1, MESSAGE_SOURCE::PART};
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
        created_tasks_v.push_back({task_factory::get_type<Type>(), data, const_data});
        return {created_tasks_v.size() - 1, TASK_SOURCE::CREATED};
    }

    template<class Type>
    local_task_id task_environment::create_child_task(const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data)
    {
        res.created_tasks().push_back({task_factory::get_type<Type>(), data, const_data});
        return {res.created_tasks().size() - 1, TASK_SOURCE::CHILD};
    }

}

#endif // __BASIC_TASK_H__
