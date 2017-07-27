#ifndef SHARE_VM_GC_IMPLEMENTATION_PARALLELSCAVENGE_EXTERNALGC_HPP
#define SHARE_VM_GC_IMPLEMENTATION_PARALLELSCAVENGE_EXTERNALGC_HPP

#include "runtime/thread.hpp"
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string>
#include <fstream>
#include <sys/ioctl.h>
#include <sys/poll.h>
#include <sys/time.h>
#include <errno.h>
#include <unistd.h>
#include <sys/resource.h>
#include <time.h>

class ExternalGCStats {
 public:
  bool pmfailure;
  int null_alloc_count;
  double null_alloc_sleep_time;
  size_t shrink_rss_size;
  int shrink_rss_count;
  size_t expand_size;
  int expand_count;
  bool doneinit;
  bool in_triggered_gc;
  size_t lastgcsaved;
  //std::string opstats_before_last_nopm_ygc;
  void initialize();
};

class ExternalTriggerThread: public JavaThread {
  friend class VMStructs;
 public:
  ExternalTriggerThread(std::string type);
  std::string _type;
  void run();
  char* name() const { return (char*)"externaltrigger"; }
};

class ExternalGCThread: public Thread {
  friend class VMStructs;
 public:
  ExternalGCThread(std::string type);
  std::string _type;
  void run();
  char* name() const { return (char*)"externalgc"; }
};

class ListenThread: public Thread {
  friend class VMStructs;
 public:
  ListenThread();
  void run();
  char* name() const { return (char*)"listen"; }
};

# endif
