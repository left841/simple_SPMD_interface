#ifndef __MESSAGE_H__
#define __MESSAGE_H__

#include <queue>
#include <vector>
#include "mpi.h"
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

        static int my_id;
        static int my_part_id;

    public:

        message_creator();
        ~message_creator();

        message* get_message(message::init_info_base* info);
        message* get_part_from(message* p, message::part_info_base* info);
        message::init_info_base* get_init_info();
        message::part_info_base* get_part_info();

        static int get_id();
        static int get_part_id();

        friend class message_factory;
    };

    template<typename Type>
    int message_creator<Type>::my_id = -1;

    template<typename Type>
    int message_creator<Type>::my_part_id = -1;

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
    int message_creator<Type>::get_id()
    { return my_id; }

    template<typename Type>
    int message_creator<Type>::get_part_id()
    { return my_part_id; }

    class message_factory
    {
    private:

        static std::vector<message_creator_base*> v;
        static std::vector<message_creator_base*> v_part;
        message_factory();

    public:

        template<typename Type>
        static void add();

        template<typename Type>
        static void add_part();

        static message* get(size_t id, message::init_info_base* info);
        static message* get_part(size_t id, message* p, message::part_info_base* info);
        static message::init_info_base* get_info(size_t id);
        static message::part_info_base* get_part_info(size_t id);
    };

    template<typename Type>
    void message_factory::add()
    {
        if (message_creator<Type>::my_id >= 0)
            return;
        message_creator<Type>::my_id = v.size();
        v.push_back(new message_creator<Type>());
    }

    template<typename Type>
    void message_factory::add_part()
    {
        if (message_creator<Type>::my_part_id >= 0)
            return;
        message_creator<Type>::my_part_id = v_part.size();
        v_part.push_back(new message_creator<Type>());
    }

}

#endif // __MESSAGE_H__
