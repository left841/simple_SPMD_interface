#include "message_factory.h"

namespace auto_parallel
{

    // init
    message_init_factory::creator_base::creator_base()
    { }

    message_init_factory::creator_base::~creator_base()
    { }

    std::vector<message_init_factory::creator_base*>& message_init_factory::message_vec()
    {
        static std::vector<creator_base*> v;
        return v;
    }

    message* message_init_factory::get(message_type id, const sendable& info)
    { return message_vec().at(id)->get_message(info); }

    sendable* message_init_factory::get_info(message_type id)
    { return message_vec().at(id)->get_info(); }

    // child
    message_child_factory::creator_base::creator_base()
    { }

    message_child_factory::creator_base::~creator_base()
    { }

    std::vector<message_child_factory::creator_base*>& message_child_factory::message_vec()
    {
        static std::vector<creator_base*> v;
        return v;
    }

    message* message_child_factory::get(message_type id, const message& parent, const sendable& info)
    { return message_vec().at(id)->get_message(parent, info); }

    message* message_child_factory::get(message_type id, const sendable& info)
    { return message_vec().at(id)->get_message(info); }

    sendable* message_child_factory::get_info(message_type id)
    { return message_vec().at(id)->get_info(); }

    void message_child_factory::include(message_type id, message& parent, const message& child, const sendable& info)
    { message_vec().at(id)->include(parent, child, info); }

}
