#ifndef __BASIC_TASK_H__
#define __BASIC_TASK_H__

#include "parallel_defs.h"
#include "message.h"
#include <vector>
#include <functional>

namespace auto_parallel
{

    class task;

    class task_creator_base
    {
    public:

        task_creator_base();
        virtual ~task_creator_base();

        virtual task* get_task(std::vector<message*>& data, std::vector<const message*>& c_data) = 0;

    };

    template<typename Type>
    class task_creator: public task_creator_base
    {
    private:

        static task_type my_type;

    public:

        task_creator();
        ~task_creator();

        task* get_task(std::vector<message*>& data, std::vector<const message*>& c_data);
        static task_type get_type();

        friend class task_factory;
    };

    template<typename Type>
    task_type task_creator<Type>::my_type = TASK_TYPE_UNDEFINED;

    template<typename Type>
    task_creator<Type>::task_creator()
    { }

    template<typename Type>
    task_creator<Type>::~task_creator()
    { }

    template<typename Type>
    task* task_creator<Type>::get_task(std::vector<message*>& data, std::vector<const message*>& c_data)
    { return new Type(data, c_data); }

    template<typename Type>
    task_type task_creator<Type>::get_type()
    { return my_type; }

    enum class MESSAGE_SOURCE: size_t
    {
        TASK_ARG, TASK_ARG_C,
        CREATED, PART
    };

    typedef size_t local_task_id;

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
        message::init_info_base* iib;
    };

    struct message_part_data
    {
        message_type type;
        local_message_id sourse;
        message::init_info_base* iib;
        message::part_info_base* pib;
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
        task_result res;

    public:

        task_environment(task_data& td);
        task_environment(task_data&& td);

        template<class Type>
        local_task_id create_task(const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data);
        template<class Type>
        local_message_id create_message(message::init_info_base* iib);
        template<class Type>
        local_message_id create_message(message::init_info_base* iib, message::part_info_base* pib, local_message_id source);

        task_result& get_result();
        std::vector<task_data>& created_tasks();
        std::vector<message_data>& created_messages();
        std::vector<message_part_data>& created_parts();

        local_message_id get_arg_id(size_t n);
        local_message_id get_c_arg_id(size_t n);
        task_data get_this_task_data();

    };

    template<class Type>
    local_task_id task_environment::create_task(const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data)
    {
        res.created_tasks().push_back({task_creator<Type>::get_type(), data, const_data});
        return res.created_tasks().size() - 1;
    }

    template<class Type>
    local_message_id task_environment::create_message(message::init_info_base* iib)
    {
        res.created_messages().push_back({message_creator<Type>::get_id(), iib});
        return {created_messages.size() - 1, MESSAGE_SOURCE::CREATED};
    }

    template<class Type>
    local_message_id task_environment::create_message(message::init_info_base* iib, message::part_info_base* pib, local_message_id source)
    {
        res.created_parts().push_back({message_creator<Type>::get_part_id(), source, iib, pib});
        return {res.created_parts().size() - 1, MESSAGE_SOURCE::PART};
    }

    class task
    {
    protected:

        std::vector<message*> data;
        std::vector<const message*> c_data;

    public:

        task();
        task(std::vector<message*>& mes_v);
        task(std::vector<message*>& mes_v, std::vector<const message*>& c_mes_v);
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

        static std::vector<task_creator_base*> v;
        task_factory() = delete;

    public:

        template<typename Type>
        static void add();

        static task* get(task_type id, std::vector<message*>& data, std::vector<const message*>& c_data);

    };

    template<typename Type>
    void task_factory::add()
    {
        if (task_creator<Type>::get_type() != TASK_TYPE_UNDEFINED)
            return;
        task_creator<Type>::my_type = static_cast<task_type>(v.size());
        v.push_back(new task_creator<Type>);
    }

}

#endif // __BASIC_TASK_H__
