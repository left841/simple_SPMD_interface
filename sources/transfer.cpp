#include "transfer.h"

namespace auto_parallel
{

    sender::sender(): tag(0)
    { }

    sender::~sender()
    { }

    void sender::send_bytes(const void* buf, int count) const
    { send(buf, count, MPI_BYTE); }

    void sender::isend_bytes(const void* buf, int count) const
    { isend(buf, count, MPI_BYTE); }

    receiver::receiver(): tag(0)
    { }

    receiver::~receiver()
    { }

    void receiver::recv_bytes(void* buf, int count) const
    { recv(buf, count, MPI_BYTE); }

    void receiver::irecv_bytes(void* buf, int count) const
    { irecv(buf, count, MPI_BYTE); }

    int receiver::probe_bytes() const
    { return probe(MPI_BYTE); }

    //send-recv specifications
    // char
    template<>
    void sender::send<char>(const char* buf, int size) const
    { send(buf, size, MPI_SIGNED_CHAR); }

    template<>
    void sender::isend<char>(const char* buf, int size) const
    { isend(buf, size, MPI_SIGNED_CHAR); }

    template<>
    void receiver::recv<char>(char* buf, int size) const
    { recv(buf, size, MPI_SIGNED_CHAR); }

    template<>
    void receiver::irecv<char>(char* buf, int size) const
    { irecv(buf, size, MPI_SIGNED_CHAR); }

    template<>
    int receiver::probe<char>() const
    { return probe(MPI_SIGNED_CHAR); }

    // unsigned char
    template<>
    void sender::send<unsigned char>(const unsigned char* buf, int size) const
    { send(buf, size, MPI_UNSIGNED_CHAR); }

    template<>
    void sender::isend<unsigned char>(const unsigned char* buf, int size) const
    { isend(buf, size, MPI_UNSIGNED_CHAR); }

    template<>
    void receiver::recv<unsigned char>(unsigned char* buf, int size) const
    { recv(buf, size, MPI_UNSIGNED_CHAR); }

    template<>
    void receiver::irecv<unsigned char>(unsigned char* buf, int size) const
    { irecv(buf, size, MPI_UNSIGNED_CHAR); }

    template<>
    int receiver::probe<unsigned char>() const
    { return probe(MPI_UNSIGNED_CHAR); }

    // short
    template<>
    void sender::send<short>(const short* buf, int size) const
    { send(buf, size, MPI_SHORT); }

    template<>
    void sender::isend<short>(const short* buf, int size) const
    { isend(buf, size, MPI_SHORT); }

    template<>
    void receiver::recv<short>(short* buf, int size) const
    { recv(buf, size, MPI_SHORT); }

    template<>
    void receiver::irecv<short>(short* buf, int size) const
    { irecv(buf, size, MPI_SHORT); }

    template<>
    int receiver::probe<short>() const
    { return probe(MPI_SHORT); }

    // unsigned short
    template<>
    void sender::send<unsigned short>(const unsigned short* buf, int size) const
    { send(buf, size, MPI_UNSIGNED_SHORT); }

    template<>
    void sender::isend<unsigned short>(const unsigned short* buf, int size) const
    { isend(buf, size, MPI_UNSIGNED_SHORT); }

    template<>
    void receiver::recv<unsigned short>(unsigned short* buf, int size) const
    { recv(buf, size, MPI_UNSIGNED_SHORT); }

    template<>
    void receiver::irecv<unsigned short>(unsigned short* buf, int size) const
    { irecv(buf, size, MPI_UNSIGNED_SHORT); }

    template<>
    int receiver::probe<unsigned short>() const
    { return probe(MPI_UNSIGNED_SHORT); }

    // int
    template<>
    void sender::send<int>(const int* buf, int size) const
    { send(buf, size, MPI_INT); }

    template<>
    void sender::isend<int>(const int* buf, int size) const
    { isend(buf, size, MPI_INT); }

    template<>
    void receiver::recv<int>(int* buf, int size) const
    { recv(buf, size, MPI_INT); }

    template<>
    void receiver::irecv<int>(int* buf, int size) const
    { irecv(buf, size, MPI_INT); }

    template<>
    int receiver::probe<int>() const
    { return probe(MPI_INT); }

    // unsigned
    template<>
    void sender::send<unsigned>(const unsigned* buf, int size) const
    { send(buf, size, MPI_UNSIGNED); }

    template<>
    void sender::isend<unsigned>(const unsigned* buf, int size) const
    { isend(buf, size, MPI_UNSIGNED); }

    template<>
    void receiver::recv<unsigned>(unsigned* buf, int size) const
    { recv(buf, size, MPI_UNSIGNED); }

    template<>
    void receiver::irecv<unsigned>(unsigned* buf, int size) const
    { irecv(buf, size, MPI_UNSIGNED); }

    template<>
    int receiver::probe<unsigned>() const
    { return probe(MPI_UNSIGNED); }

    // long
    template<>
    void sender::send<long>(const long* buf, int size) const
    { send(buf, size, MPI_LONG); }

    template<>
    void sender::isend<long>(const long* buf, int size) const
    { isend(buf, size, MPI_LONG); }

    template<>
    void receiver::recv<long>(long* buf, int size) const
    { recv(buf, size, MPI_LONG); }

    template<>
    void receiver::irecv<long>(long* buf, int size) const
    { irecv(buf, size, MPI_LONG); }

    template<>
    int receiver::probe<long>() const
    { return probe(MPI_LONG); }

    // unsigned long
    template<>
    void sender::send<unsigned long>(const unsigned long* buf, int size) const
    { send(buf, size, MPI_UNSIGNED_LONG); }

    template<>
    void sender::isend<unsigned long>(const unsigned long* buf, int size) const
    { isend(buf, size, MPI_UNSIGNED_LONG); }

    template<>
    void receiver::recv<unsigned long>(unsigned long* buf, int size) const
    { recv(buf, size, MPI_UNSIGNED_LONG); }

    template<>
    void receiver::irecv<unsigned long>(unsigned long* buf, int size) const
    { irecv(buf, size, MPI_UNSIGNED_LONG); }

    template<>
    int receiver::probe<unsigned long>() const
    { return probe(MPI_UNSIGNED_LONG); }

    // long long
    template<>
    void sender::send<long long>(const long long* buf, int size) const
    { send(buf, size, MPI_LONG_LONG); }

    template<>
    void sender::isend<long long>(const long long* buf, int size) const
    { isend(buf, size, MPI_LONG_LONG); }

    template<>
    void receiver::recv<long long>(long long* buf, int size) const
    { recv(buf, size, MPI_LONG_LONG); }

    template<>
    void receiver::irecv<long long>(long long* buf, int size) const
    { irecv(buf, size, MPI_LONG_LONG); }

    template<>
    int receiver::probe<long long>() const
    { return probe(MPI_LONG_LONG); }

    // unsigned long long
    template<>
    void sender::send<unsigned long long>(const unsigned long long* buf, int size) const
    { send(buf, size, MPI_UNSIGNED_LONG_LONG); }

    template<>
    void sender::isend<unsigned long long>(const unsigned long long* buf, int size) const
    { isend(buf, size, MPI_UNSIGNED_LONG_LONG); }

    template<>
    void receiver::recv<unsigned long long>(unsigned long long* buf, int size) const
    { recv(buf, size, MPI_UNSIGNED_LONG_LONG); }

    template<>
    void receiver::irecv<unsigned long long>(unsigned long long* buf, int size) const
    { irecv(buf, size, MPI_UNSIGNED_LONG_LONG); }

    template<>
    int receiver::probe<unsigned long long>() const
    { return probe(MPI_UNSIGNED_LONG_LONG); }

    // float
    template<>
    void sender::send<float>(const float* buf, int size) const
    { send(buf, size, MPI_FLOAT); }

    template<>
    void sender::isend<float>(const float* buf, int size) const
    { isend(buf, size, MPI_FLOAT); }

    template<>
    void receiver::recv<float>(float* buf, int size) const
    { recv(buf, size, MPI_FLOAT); }

    template<>
    void receiver::irecv<float>(float* buf, int size) const
    { irecv(buf, size, MPI_FLOAT); }

    template<>
    int receiver::probe<float>() const
    { return probe(MPI_FLOAT); }

    // double
    template<>
    void sender::send<double>(const double* buf, int size) const
    { send(buf, size, MPI_DOUBLE); }

    template<>
    void sender::isend<double>(const double* buf, int size) const
    { isend(buf, size, MPI_DOUBLE); }

    template<>
    void receiver::recv<double>(double* buf, int size) const
    { recv(buf, size, MPI_DOUBLE); }

    template<>
    void receiver::irecv<double>(double* buf, int size) const
    { irecv(buf, size, MPI_DOUBLE); }

    template<>
    int receiver::probe<double>() const
    { return probe(MPI_DOUBLE); }

    // long double
    template<>
    void sender::send<long double>(const long double* buf, int size) const
    { send(buf, size, MPI_LONG_DOUBLE); }

    template<>
    void sender::isend<long double>(const long double* buf, int size) const
    { isend(buf, size, MPI_LONG_DOUBLE); }

    template<>
    void receiver::recv<long double>(long double* buf, int size) const
    { recv(buf, size, MPI_LONG_DOUBLE); }

    template<>
    void receiver::irecv<long double>(long double* buf, int size) const
    { irecv(buf, size, MPI_LONG_DOUBLE); }

    template<>
    int receiver::probe<long double>() const
    { return probe(MPI_LONG_DOUBLE); }

}
