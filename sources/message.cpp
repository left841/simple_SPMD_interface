#include "message.h"

namespace auto_parallel
{

    message_creator_base::message_creator_base()
    { }

    message_creator_base::~message_creator_base()
    { }

    sendable::sendable()
    { }

    sendable::~sendable()
    { }

    void sendable::wait_requests()
    {
        while (req_q.size())
        {
            MPI_Wait(&req_q.front(), MPI_STATUS_IGNORE);
            req_q.pop();
        }
    }

    message::message(): sendable()
    { }

    message::~message()
    { }

    std::vector<message_creator_base*> message_factory::v;
    std::vector<message_creator_base*> message_factory::v_part;

    message* message_factory::get(message_type id, message::init_info_base* info)
    { return v[id]->get_message(info); }

    message* message_factory::get_part(message_type id, message* p, message::part_info_base* info)
    { return v_part[id]->get_part_from(p, info); }

    message::init_info_base* message_factory::message_factory::get_info(message_type id)
    { return v[id]->get_init_info(); }

    message::part_info_base* message_factory::get_part_info(message_type id)
    { return v_part[id]->get_part_info(); }

}
