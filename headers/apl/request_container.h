#ifndef __REQUEST_CONTAINER_H__
#define __REQUEST_CONTAINER_H__

#include <cassert>
#include <algorithm>
#include <iterator>
#include <vector>
#include "apl/parallel_defs.h"

namespace apl
{

    class request_block
    {
    private:
        std::vector<MPI_Request> req_vec;

    public:
        request_block();
        ~request_block();

        request_block(const request_block& r) = delete;
        request_block& operator=(const request_block& r) = delete;
        request_block(request_block&& r) noexcept = default;
        request_block& operator=(request_block&& r) noexcept = default;

        void store(MPI_Request req);
        void store(request_block& req);
        bool test_all();
        void wait_all();
        void cancel_all();
        void free_all();

        size_t size() const;
        MPI_Request* data();
        const MPI_Request* data() const;
        void clear();
    };


    class request_container
    {
    private:
        std::vector<MPI_Request> req_vec;
        std::vector<size_t> block_pos;

    public:
        request_container();
        ~request_container();

        request_container(const request_container& r) = delete;
        request_container& operator=(const request_container& r) = delete;
        request_container(request_container&& r) noexcept = default;
        request_container& operator=(request_container&& r) noexcept = default;
        
        void store(MPI_Request req);
        void store(request_block& req);

        bool test_one();
        void wait_one();
        void cancel_one();
        void free_one();

        bool test_all();
        void wait_all();
        void cancel_all();
        void free_all();
    };

}

#endif // __REQUEST_CONTAINER_H__
