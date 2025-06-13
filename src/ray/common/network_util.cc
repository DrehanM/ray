// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/common/network_util.h"

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/util/logging.h"

using boost::asio::ip::tcp;

#include <dirent.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <regex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

struct SocketEntry {
  uint16_t port;
  std::string state;
  std::string inode;
};

std::vector<SocketEntry> ParseProcNet(const std::string &path) {
  std::vector<SocketEntry> results;
  std::ifstream f(path);
  std::string line;
  std::getline(f, line);  // Skip header

  while (std::getline(f, line)) {
    std::istringstream iss(line);
    std::string sl, local_address, rem_address, state, txq, rxq, tr, tm_when, retrnsmt,
        uid, timeout, inode;
    iss >> sl >> local_address >> rem_address >> state >> txq >> rxq >> tr >> tm_when >>
        retrnsmt >> uid >> timeout >> inode;

    // Extract port
    size_t colon = local_address.find(':');
    std::string port_hex = local_address.substr(colon + 1);
    uint16_t port = std::stoul(port_hex, nullptr, 16);

    results.push_back({port, state, inode});
  }
  return results;
}

std::string FindPidByInode(const std::string &inode) {
  DIR *proc = opendir("/proc");
  if (!proc) return {};

  struct dirent *dent;
  while ((dent = readdir(proc)) != nullptr) {
    if (!isdigit(dent->d_name[0])) continue;
    std::string pid = dent->d_name;
    std::string fd_path = "/proc/" + pid + "/fd";
    DIR *fd_dir = opendir(fd_path.c_str());
    if (!fd_dir) continue;

    struct dirent *fd_entry;
    while ((fd_entry = readdir(fd_dir)) != nullptr) {
      if (fd_entry->d_type != DT_LNK) continue;
      std::string link = fd_path + "/" + fd_entry->d_name;
      char buf[1024];
      ssize_t len = readlink(link.c_str(), buf, sizeof(buf) - 1);
      if (len != -1) {
        buf[len] = '\0';
        std::string target = buf;
        RAY_LOG(ERROR) << "Checking link: " << link << " -> " << target;
        if (target.find("socket:[" + inode + "]") != std::string::npos) {
          closedir(fd_dir);
          closedir(proc);
          return pid;
        }
      }
    }
    closedir(fd_dir);
  }
  closedir(proc);
  return {};
}

std::string TcpStateToString(const std::string &code) {
  static std::unordered_map<std::string, std::string> states = {{"01", "ESTABLISHED"},
                                                                {"02", "SYN_SENT"},
                                                                {"03", "SYN_RECV"},
                                                                {"04", "FIN_WAIT1"},
                                                                {"05", "FIN_WAIT2"},
                                                                {"06", "TIME_WAIT"},
                                                                {"07", "CLOSED"},
                                                                {"08", "CLOSE_WAIT"},
                                                                {"09", "LAST_ACK"},
                                                                {"0A", "LISTEN"},
                                                                {"0B", "CLOSING"}};
  auto it = states.find(code);
  return (it != states.end()) ? it->second : "UNKNOWN";
}

std::string GetPortState(uint16_t port) {
  auto entries = ParseProcNet("/proc/net/tcp");
  auto entries6 = ParseProcNet("/proc/net/tcp6");
  entries.insert(entries.end(), entries6.begin(), entries6.end());

  for (const auto &entry : entries) {
    if (entry.port == port && entry.state == "0A") {  // LISTEN
      std::string pid = FindPidByInode(entry.inode);
      if (!pid.empty()) {
        return " Port " + std::to_string(port) + " is LISTENING by PID: " + pid;
      } else {
        return " Port " + std::to_string(port) + " is LISTENING but PID not found.";
      }
    }
  }

  return "Port " + std::to_string(port) + " not found in LISTEN state.";
}

bool CheckPortFree(int port) {
  instrumented_io_context io_service;
  tcp::socket socket(io_service);
  socket.open(boost::asio::ip::tcp::v4());

  // Disable SO_REUSEADDR
  boost::asio::socket_base::reuse_address option(false);
  socket.set_option(option);

  boost::system::error_code ec;
  socket.bind(boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port), ec);
  socket.close();
  return !ec.failed();
}
