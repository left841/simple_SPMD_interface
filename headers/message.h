#ifndef __MESSAGE_H__
#define __MESSAGE_H__

#include <queue>
#include <vector>
#include "mpi.h"
#include "parallel_defs.h"
#include "transfer.h"

namespace auto_parallel
{

    class sendable
    {
    private:

        std::queue<MPI_Request> req_q;

    public:

        sendable();
        virtual ~sendable();

        virtual void send(const sender& se) = 0;
        virtual void recv(const receiver& re) = 0;

        void wait_requests();

        friend class intracomm;
    };

    class message: public sendable
    {
    public:

        struct init_info_base: public sendable
        { };

        struct part_info_base: public sendable
        { };

        message();
        virtual ~message();
    };

    class message_creator_base
    {
    public:

        message_creator_base();
        virtual ~message_creator_base();

        virtual message* get_message(message::init_info_base* info) = 0;
        virtual message* get_part_from(message* p, message::part_info_base* info) = 0;
        virtual message::init_info_base* get_init_info() = 0;
        virtual message::part_info_base* get_part_info() = 0;
    };

    template<typename Type>
    class message_creator: public message_creator_base
    {
    private:

        static message_type my_type;
        static message_type my_part_type;

    public:

        message_creator();
        ~message_creator();

        message* get_message(message::init_info_base* info);
        message* get_part_from(message* p, message::part_info_base* info);
        message::init_info_base* get_init_info();
        message::part_info_base* get_part_info();

        static message_type get_id();
        static message_type get_part_id();

        friend class message_factory;
    };

    template<typename Type>
    message_type message_creator<Type>::my_type = MESSAGE_TYPE_UNDEFINED;

    template<typename Type>
    message_type message_creator<Type>::my_part_type = MESSAGE_TYPE_UNDEFINED;

    template<typename Type>
    message_creator<Type>::message_creator()
    { }

    template<typename Type>
    message_creator<Type>::~message_creator()
    { }

    template<class Type>
    message* message_creator<Type>::get_message(message::init_info_base* info)
    {
        typename Type::init_info* p = dynamic_cast<typename Type::init_info*>(info);
        return new Type(p);
    }

    template<typename Type>
    message* message_creator<Type>::get_part_from(message* p, message::part_info_base* info)
    {
        typename Type::part_info* q = dynamic_cast<typename Type::part_info*>(info);
        return new Type(p, q);
    }

    template<typename Type>
    message::init_info_base* message_creator<Type>::get_init_info()
    { return new typename Type::init_info; }

    template<typename Type>
    message::part_info_base* message_creator<Type>::get_part_info()
    { return new typename Type::part_info; }

    template<typename Type>
    message_type message_creator<Type>::get_id()
    { return my_type; }

    template<typename Type>
    message_type message_creator<Type>::get_part_id()
    { return my_part_type; }

    class message_factory
    {
    private:

        static std::vector<message_creator_base*> v;
        static std::vector<message_creator_base*> v_part;
        message_factory() = delete;

    public:

        template<typename Type>
        static void add();

        template<typename Type>
        static void add_part();

        static message* get(message_type id, message::init_info_base* info);
        static message* get_part(message_type id, message* p, message::part_info_base* info);
        static message::init_info_base* get_info(message_type id);
        static message::part_info_base* get_part_info(message_type id);
    };

    template<typename Type>
    void message_factory::add()
    {
        if (message_creator<Type>::my_type != MESSAGE_TYPE_UNDEFINED)
            return;
        message_creator<Type>::my_type = static_cast<message_type>(v.size());
        v.push_back(new message_creator<Type>());
    }

    template<typename Type>
    void message_factory::add_part()
    {
        if (message_creator<Type>::my_part_type != MESSAGE_TYPE_UNDEFINED)
            return;
        message_creator<Type>::my_part_type = static_cast<message_type>(v_part.size());
        v_part.push_back(new message_creator<Type>());
    }

}

#endif // __MESSAGE_H__
