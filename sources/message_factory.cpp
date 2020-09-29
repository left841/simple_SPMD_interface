#include "message_factory.h"

namespace apl
{

    // init
    message_init_factory::creator_base::creator_base()
    { }

    message_init_factory::creator_base::~creator_base()
    { }

    std::vector<std::unique_ptr<message_init_factory::creator_base>>& message_init_factory::message_vec()
    {
        static std::vector<std::unique_ptr<creator_base>> v;
        return v;
    }

    message* message_init_factory::get(message_type id, const std::vector<message*>& info)
    { return message_vec().at(id)->get_message(info); }

    std::vector<message*> message_init_factory::get_info(message_type id)
    { return message_vec().at(id)->get_info(); }

    // child
    message_child_factory::creator_base::creator_base()
    { }

    message_child_factory::creator_base::~creator_base()
    { }

    std::vector<std::unique_ptr<message_child_factory::creator_base>>& message_child_factory::message_vec()
    {
        static std::vector<std::unique_ptr<creator_base>> v;
        return v;
    }

    message* message_child_factory::get(message_type id, const message* parent, const std::vector<message*>& info)
    { return message_vec().at(id)->get_message(parent, info); }

    message* message_child_factory::get(message_type id, const std::vector<message*>& info)
    { return message_vec().at(id)->get_message(info); }

    std::vector<message*> message_child_factory::get_info(message_type id)
    { return message_vec().at(id)->get_info(); }

    void message_child_factory::include(message_type id, message* parent, const message* child, const std::vector<message*>& info)
    { message_vec().at(id)->include(parent, child, info); }

}
