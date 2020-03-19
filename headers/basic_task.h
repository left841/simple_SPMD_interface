#ifndef __BASIC_TASK_H__
#define __BASIC_TASK_H__

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

        static int my_id;

    public:

        task_creator();
        ~task_creator();

        task* get_task(std::vector<message*>& data, std::vector<const message*>& c_data);
        static int get_type();

        friend class task_factory;
    };

    template<typename Type>
    int task_creator<Type>::my_id = -1;

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
    int task_creator<Type>::get_type()
    { return my_id; }

    enum class MESSAGE_SOURCE
    {
        TASK_ARG, TASK_ARG_C,
        CREATED, PART
    };

    struct local_message_id
    {
        int id;
        MESSAGE_SOURCE ms;
    };

    struct task_data
    {
        int type;
        std::vector<local_message_id> data, c_data;
    };

    struct message_data
    {
        int type;
        message::init_info_base* iib;
    };

    struct message_part_data
    {
        int type;
        local_message_id sourse;
        message::init_info_base* iib;
        message::part_info_base* pib;
    };

    class task_environment
    {
    private:

        task_data this_task;
        std::vector<task_data> created_tasks;
        std::vector<message_data> created_messages;
        std::vector<message_part_data> created_parts;

    public:

        task_environment(task_data& td);
        task_environment(task_data&& td);

        template<class Type>
        int create_task(const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data);
        template<class Type>
        local_message_id create_message(message::init_info_base* iib);
        template<class Type>
        local_message_id create_message(message::init_info_base* iib, message::part_info_base* pib, local_message_id sourse);

        std::vector<task_data>& get_c_tasks();
        std::vector<message_data>& get_c_messages();
        std::vector<message_part_data>& get_c_parts();

        local_message_id get_arg_id(int n);
        local_message_id get_c_arg_id(int n);
        task_data get_this_task_data();

    };

    template<class Type>
    int task_environment::create_task(const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data)
    {
        created_tasks.push_back({task_creator<Type>::get_type(), data, const_data});
        return static_cast<int>(created_tasks.size() - 1);
    }

    template<class Type>
    local_message_id task_environment::create_message(message::init_info_base* iib)
    {
        created_messages.push_back({message_creator<Type>::get_id(), iib});
        return {static_cast<int>(created_messages.size() - 1), MESSAGE_SOURCE::CREATED};
    }

    template<class Type>
    local_message_id task_environment::create_message(message::init_info_base* iib, message::part_info_base* pib, local_message_id sourse)
    {
        created_parts.push_back({message_creator<Type>::get_part_id(), sourse, iib, pib});
        return {static_cast<int>(created_parts.size() - 1), MESSAGE_SOURCE::PART};
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
        task_factory();

    public:

        template<typename Type>
        static void add();

        static task* get(size_t id, std::vector<message*>& data, std::vector<const message*>& c_data);

    };

    template<typename Type>
    void task_factory::add()
    {
        if (task_creator<Type>::get_type() > -1)
            return;
        task_creator<Type>::my_id = static_cast<int>(v.size());
        v.push_back(new task_creator<Type>);
    }

}

#endif // __BASIC_TASK_H__
