#pragma once
#include <exception>
#include <string>
#include <sstream>

class MyException : public std::exception
{
public:
    template<typename... Types>
    MyException(Types... args) noexcept :
        std::exception()
    {
        std::stringstream os;
        int dummy[sizeof...(args)] = { (os << args, 0)... };
        m_error = os.str();
    }

    std::string error() const {
        return m_error;
    }

private:
    std::string m_error;
};
