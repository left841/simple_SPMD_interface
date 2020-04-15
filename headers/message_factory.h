#ifndef __MESSAGE_FACTORY_H__
#define __MESSAGE_FACTORY_H__

#include "message.h"

namespace apl
{

    enum class MESSAGE_FACTORY_TYPE: size_t
    {
        UNDEFINED,
        SIMPLE, COPY, INIT, CHILD, PART
    };

    // init
    class message_init_factory
    {
    private:

        class creator_base
        {
        public:

            creator_base();
            virtual ~creator_base();

            virtual message* get_message(const sendable& info) = 0;
            virtual sendable* get_info() = 0;

        };

        template<typename Type, typename InfoType>
        struct id
        {
            message_type value;
            id();
        };

        template<typename Type, typename InfoType>
        class creator: public creator_base
        {
        private:

            static id<Type, InfoType> my_type;

        public:

            creator();
            ~creator();

            message* get_message(const sendable& info);
            sendable* get_info();

            static message_type get_type();

            friend class message_init_factory;
        };

        message_init_factory() = delete;
        static std::vector<creator_base*>& message_vec();

        template<typename Type, typename InfoType>
        static task_type add();

    public:

        static message* get(message_type id, const sendable& info);
        static sendable* get_info(message_type id);

        template<typename Type, typename InfoType>
        static message_type get_type();

    };

    template<typename Type, typename InfoType>
    message_init_factory::id<Type, InfoType>::id(): value(message_init_factory::add<Type, InfoType>())
    { }

    template<typename Type, typename InfoType>
    message_init_factory::creator<Type, InfoType>::creator()
    { }

    template<typename Type, typename InfoType>
    message_init_factory::creator<Type, InfoType>::~creator()
    { }

    template<typename Type, typename InfoType>
    message* message_init_factory::creator<Type, InfoType>::get_message(const sendable& info)
    { return new Type(dynamic_cast<const InfoType&>(info)); }

    template<typename Type, typename InfoType>
    sendable* message_init_factory::creator<Type, InfoType>::get_info()
    { return new InfoType(); }

    template<typename Type, typename InfoType>
    message_init_factory::id<Type, InfoType> message_init_factory::creator<Type, InfoType>::my_type;

    template<typename Type, typename InfoType>
    message_type message_init_factory::creator<Type, InfoType>::get_type()
    { return my_type.value; }

    template<typename Type, typename InfoType>
    message_type message_init_factory::add()
    {
        message_vec().push_back(new creator<Type, InfoType>());
        return message_vec().size() - 1;
    }

    template<typename Type, typename InfoType>
    message_type message_init_factory::get_type()
    { return creator<Type, InfoType>::get_type(); }

    // child
    class message_child_factory
    {
    private:

        class creator_base
        {
        public:

            creator_base();
            virtual ~creator_base();

            virtual message* get_message(const message& parent, const sendable& info) = 0;
            virtual message* get_message(const sendable& info) = 0;
            virtual sendable* get_info() = 0;
            virtual void include(message& parent, const message& child, const sendable& info) = 0;

        };

        template<typename Type, typename ParentType, typename InfoType>
        struct id
        {
            message_type value;
            id();
        };

        template<typename Type, typename ParentType, typename InfoType>
        class creator: public creator_base
        {
        private:

            static id<Type, ParentType, InfoType> my_type;

        public:

            creator();
            ~creator();

            message* get_message(const message& parent, const sendable& info);
            message* get_message(const sendable& info);
            sendable* get_info();
            void include(message& parent, const message& child, const sendable& info);

            static message_type get_type();

            friend class message_child_factory;
        };

        message_child_factory() = delete;
        static std::vector<creator_base*>& message_vec();

        template<typename Type, typename ParentType, typename InfoType>
        static message_type add();

    public:

        static message* get(message_type id, const message& parent, const sendable& info);
        static message* get(message_type id, const sendable& info);
        static sendable* get_info(message_type id);
        static void include(message_type id, message& parent, const message& child, const sendable& info);

        template<typename Type, typename ParentType, typename InfoType>
        static message_type get_type();

    };

    template<typename Type, typename ParentType, typename InfoType>
    message_child_factory::id<Type, ParentType, InfoType>::id(): value(message_child_factory::add<Type, ParentType, InfoType>())
    { }

    template<typename Type, typename ParentType, typename InfoType>
    message_child_factory::creator<Type, ParentType, InfoType>::creator()
    { }

    template<typename Type, typename ParentType, typename InfoType>
    message_child_factory::creator<Type, ParentType, InfoType>::~creator()
    { }

    template<typename Type, typename ParentType, typename InfoType>
    message* message_child_factory::creator<Type, ParentType, InfoType>::get_message(const message& parent, const sendable& info)
    { return new Type(dynamic_cast<const ParentType&>(parent), dynamic_cast<const InfoType&>(info)); }

    template<typename Type, typename ParentType, typename InfoType>
    message* message_child_factory::creator<Type, ParentType, InfoType>::get_message(const sendable& info)
    { return new Type(dynamic_cast<const InfoType&>(info)); }

    template<typename Type, typename ParentType, typename InfoType>
    sendable* message_child_factory::creator<Type, ParentType, InfoType>::get_info()
    { return new InfoType(); }

    template<typename Type, typename ParentType, typename InfoType>
    void message_child_factory::creator<Type, ParentType, InfoType>::include(message& parent, const message& child, const sendable& info)
    { dynamic_cast<ParentType&>(parent).include(dynamic_cast<const Type&>(child), dynamic_cast<const InfoType&>(info)); }

    template<typename Type, typename ParentType, typename InfoType>
    message_child_factory::id<Type, ParentType, InfoType> message_child_factory::creator<Type, ParentType, InfoType>::my_type;

    template<typename Type, typename ParentType, typename InfoType>
    message_type message_child_factory::creator<Type, ParentType, InfoType>::get_type()
    { return my_type.value; }

    template<typename Type, typename ParentType, typename InfoType>
    message_type message_child_factory::add()
    {
        message_vec().push_back(new creator<Type, ParentType, InfoType>());
        return message_vec().size() - 1;
    }

    template<typename Type, typename ParentType, typename InfoType>
    message_type message_child_factory::get_type()
    { return creator<Type, ParentType, InfoType>::get_type(); }

}

#endif // __MESSAGE_FACTORY_H__
