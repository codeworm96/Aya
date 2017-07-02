#include "db.hh"

#include <unistd.h>
#include <fcntl.h>
#include <cstdio>
#include <fstream>
#include <iostream>

#define RESERVED_ORDER 10000000

void Database::recover(std::string input_file, std::string log_file)
{
  std::ifstream fin(input_file);
  std::string buf;
  std::getline(fin, buf);
  int n = std::stoi(buf);
  for (int i = 0; i < n; ++i) {
    std::getline(fin, buf);
    int x = buf.find(',');
    int y = buf.find(',', x + 1);
    std::string a = buf.substr(0, x);
    users[a].name = buf.substr(x + 1, y - x - 1);
    users[a].account_balance = std::stod(buf.substr(y + 1)) * 100;
  }
  std::getline(fin, buf);
  int m = std::stoi(buf);
  for (int i = 0; i < m; ++i) {
    std::getline(fin, buf);
    int x = buf.find(',');
    int y = buf.find(',', x + 1);
    int z = buf.find(',', y + 1);
    std::string d = buf.substr(0, x);
    commodities[d].name = buf.substr(x + 1, y - x - 1);
    commodities[d].quantity = std::stoi(buf.substr(y + 1, z - y - 1));
    commodities[d].unit_price = std::stod(buf.substr(z + 1)) * 100;
  }

  std::ifstream flog(log_file);
  orders.reserve(RESERVED_ORDER);
  order_size = 0;
  while(std::getline(flog, buf)) {
    int x = buf.find(',');
    int y = buf.find(',', x + 1);
    Order order(buf.substr(0, x), buf.substr(x + 1, y - x - 1),
                std::stoi(buf.substr(y + 1)));
    ++order_size;
    orders[order_size] = order;
    perform_order(order);
    std::cout << order_size << std::endl;
  }

  log_fd = open(log_file.c_str(), O_WRONLY|O_APPEND|O_CREAT, S_IRUSR|S_IWUSR);
}

void Database::log_write(const Order& order)
{
  int nleft = std::sprintf(buf, "%s,%s,%d\n", order.user_id.c_str(),
                           order.commodity_id.c_str(), (int)order.timestamp);
  char *p = buf;

  while (nleft > 0) {
    int nwritten = write(log_fd, p, nleft);
    if (nwritten <= 0) {
      nwritten = 0; // try again
    }
    nleft -= nwritten;
    p += nwritten;
  }

  //fdatasync(log_fd);
}

int Database::perform_order(const Order& order)
{
  User &user = users[order.user_id];
  Commodity &commodity = commodities[order.commodity_id];
  if (commodity.quantity <= 0 || user.account_balance < commodity.unit_price) {
    return 0;
  } else {
    --commodity.quantity;
    user.account_balance -= commodity.unit_price;
    return 1;
  }
}

std::pair<int, int> Database::seckill(std::string user_id, std::string commodity_id)
{
  Order order(user_id, commodity_id, std::time(nullptr));
  int result = perform_order(order);
  if (result) {
    log_write(order);
    ++order_size;
    int order_id = order_size;
    orders[order_id] = order;
    return std::make_pair(result, order_id);
  } else {
    return std::make_pair(0, -1);
  }
}
