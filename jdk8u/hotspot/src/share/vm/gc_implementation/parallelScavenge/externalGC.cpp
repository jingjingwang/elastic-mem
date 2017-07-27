#include "gc_implementation/parallelScavenge/externalGC.hpp"
#include "gc_implementation/parallelScavenge/parallelScavengeHeap.hpp"
#include "gc_implementation/parallelScavenge/vmPSOperations.hpp"
#include "runtime/vmThread.hpp"
#include "runtime/arguments.hpp"
#include <sstream>

void ExternalGCStats::initialize() { 
  this->in_triggered_gc = false;
  this->lastgcsaved = 0;
  this->pmfailure = false;
  this->null_alloc_sleep_time = 0.0;
  this->null_alloc_count = 0;
  this->shrink_rss_size = 0;
  this->shrink_rss_count = 0;
  this->expand_size = 0;
  this->expand_count = 0;
}

void send_to_socket(std::string s, pollfd fd) {
  const char* c = s.c_str();
  int total = strlen(c);
  int sent = 0;
  while (sent < total) {
    int t = send(fd.fd, c + sent, total - sent, 0);
    if (t < 0) {
      perror("send() error");
      break;
    }
    sent += t;
  }
}

void ExternalTriggerThread::run() {
  ParallelScavengeHeap* heap = (ParallelScavengeHeap*)(Universe::heap());
  ExternalGCStats* stats = heap->externalGCStats;
  unsigned int gc_count      = 0;
  unsigned int full_gc_count = 0;
  {
    MutexLocker ml(Heap_lock);
    gc_count      = heap->total_collections();
    full_gc_count = heap->total_full_collections();
  }
  size_t beforegcused = heap->young_gen()->eden_space()->used_in_bytes() + heap->old_gen()->used_in_bytes();
  if (this->_type.substr(0, 8)  == "fgc_nopm") {
    size_t move_unit = size_t(atoi(this->_type.substr(9).c_str()));
    VM_ParallelGCFull op(gc_count, full_gc_count, false, move_unit);
    VMThread::execute(&op);
  } else if (this->_type.substr(0, 6)  == "fgc_pm") {
    size_t move_unit = size_t(atoi(this->_type.substr(7).c_str()));
    VM_ParallelGCFull op(gc_count, full_gc_count, true, move_unit);
    VMThread::execute(&op);
  } else if (this->_type == "ygc") {
    VM_ParallelGCScavenge op(gc_count);
    VMThread::execute(&op);
  } else {ShouldNotReachHere();}
  size_t aftergcused = heap->young_gen()->eden_space()->used_in_bytes() + heap->old_gen()->used_in_bytes();
  stats->lastgcsaved = aftergcused < beforegcused ? beforegcused - aftergcused : 0;
  if (stats->lastgcsaved > 0)
  {
    MutexLocker ml(heap->adjustment_mutex);
    heap->adjustment_mutex->notify_all();
  }
  stats->in_triggered_gc = false;
}

void ExternalGCThread::run() {
  ExternalTriggerThread* t = new ExternalTriggerThread(this->_type);
  os::create_thread(t, os::listen_thread);
  os::start_thread(t);
  ParallelScavengeHeap* heap = (ParallelScavengeHeap*)(Universe::heap());
  ExternalGCStats* stats = heap->externalGCStats;
  while (stats->in_triggered_gc) {
    while (stats->in_triggered_gc && !heap->is_gc_active()) {
      {
        MutexLocker ml(heap->adjustment_mutex);
        heap->adjustment_mutex->notify_all();
        // to gurarantee a gc can proceed without waiting for a blocked thread to reach a safepoint
      }
      usleep(100);
    }
    usleep(10000);
  }
  delete t;
}

void ListenThread::run() {
  ParallelScavengeHeap* heap = (ParallelScavengeHeap*)(Universe::heap());
  PSYoungGen* young = heap->young_gen();
  PSOldGen* old = heap->old_gen();
  MutableSpace* eden = young->eden_space();
  MutableSpace* from = young->from_space();
  MutableSpace* to = young->to_space();
  ExternalGCStats* stats = heap->externalGCStats;

  int listener;
  if ((listener = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
    printf("Server-socket() error lol!");
    return;
  }
  int yes = 1;
  if (setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
    perror("Server-setsockopt() error lol!");
    return;
  }
  struct sockaddr_in serveraddr;
  serveraddr.sin_family = AF_INET;
  serveraddr.sin_addr.s_addr = INADDR_ANY;
  serveraddr.sin_port = htons(JVMPort);
  memset(&(serveraddr.sin_zero), '\0', 8);
  if (bind(listener, (struct sockaddr *)&serveraddr, sizeof(serveraddr)) == -1) {
    perror("Server-bind() error lol!");
    return;
  }
  if (listen(listener, 10) == -1) {
    perror("Server-listen() error lol!");
    return;
  }

  struct pollfd fds[10];
  memset(fds, 0 , sizeof(fds));
  fds[0].fd = listener;
  fds[0].events = POLLIN;
  int nfds = 1;
  tty->print_cr("listen on port %d", (int)JVMPort);
  while (true) {
    if (poll(fds, nfds, -1) < 0)
      break;
    for (int i = 0; i < nfds; i++)
      if (fds[i].revents)
        if (fds[i].fd == listener) {
          struct sockaddr_in clientaddr;
          socklen_t addrlen = sizeof(clientaddr);
          int newfd;
          if ((newfd = accept(listener, (struct sockaddr *)&clientaddr, &addrlen)) == -1) {
            perror("Server-accept() error lol!");
          } else {
            if (nfds + 1 >= 10) break;
            fds[nfds].fd = newfd;
            fds[nfds].events = POLLIN;
            nfds++;
          }
        } else {
          char buf[1024];
          int nbytes;
          std::string input = "";
          bool hasinput = false;
          while (true) {
            if ((nbytes = recv(fds[i].fd, buf, sizeof(buf), 0)) <= 0) {
              close(fds[i].fd);
              std::swap(fds[i], fds[nfds-1]);
              nfds--;
              break;
            }
            for (int j = 0; j < nbytes; ++j)
              input += buf[j];
            if (input[input.length()-1] == '\n') {
              hasinput = true;
              break;
            }
          }
          if (!hasinput) continue;
          tty->print_cr("got input %s", input.substr(0, input.length()-1).c_str());
          std::stringstream istream(input);
          std::stringstream ostream;
          ostream.clear();
          std::string key;
          while (istream >> key) {
            if (key == "ecap")
              ostream << eden->capacity_in_bytes() << "|";
            else if (key == "eused")
              ostream << eden->used_in_bytes() << "|";
            else if (key == "emax")
              ostream << young->_max_eden_size << "|";
            else if (key == "ocap")
              ostream << old->capacity_in_bytes() << "|";
            else if (key == "oused")
              ostream << old->used_in_bytes() << "|";
            else if (key == "omax")
              ostream << old->max_gen_size() << "|";
            else if (key == "fcap")
              ostream << from->capacity_in_bytes() << "|";
            else if (key == "fused")
              ostream << from->used_in_bytes() << "|";
            else if (key == "blocked_sleep")
              ostream << stats->null_alloc_sleep_time << "|";
            else if (key == "blocked_count")
              ostream << stats->null_alloc_count << "|";
            else if (key == "shrink_rss_count")
              ostream << stats->shrink_rss_count << "|";
            else if (key == "shrink_rss_size")
              ostream << stats->shrink_rss_size << "|";
            else if (key == "expand_count")
              ostream << stats->expand_count << "|";
            else if (key == "expand_size")
              ostream << stats->expand_size << "|";
            else if (key == "pmfailure")
              ostream << stats->pmfailure << "|";
            else if (key == "cpu")
              ostream << (double)clock() / CLOCKS_PER_SEC << "|";
            else if (key == "rss" || key == "private") {
              long tSize = 0, resident = 0, share = 0;
              std::ifstream buffer("/proc/self/statm");
              buffer >> tSize >> resident >> share;
              buffer.close();
              long page_size_kb = sysconf(_SC_PAGE_SIZE);
              if (key == "rss") ostream << resident * page_size_kb << "|";
              else ostream << (resident - share) * page_size_kb << "|";
            }
            else if (key == "gcactive")
              ostream << (heap->is_gc_active() | stats->in_triggered_gc) << "|";
            else if (key == "intriggeredgc")
              ostream << stats->in_triggered_gc << "|";
            else if (key == "lastgcsaved")
              ostream << stats->lastgcsaved << "|";
            /*else if (key == "opstats_nopmgc")
              ostream << stats->opstats_before_last_nopm_ygc << "|";
            else if (key == "opstats") {
              const char* name = Arguments::gc_log_filename();
              if (name != NULL) {
                std::string opstats_file = std::string(name);
                opstats_file = opstats_file.substr(0, opstats_file.find("gclog")) + "opstats";
                std::ifstream fin(opstats_file.c_str());
                if (fin) {
                  std::string line;
                  getline(fin, line);
                  fin.close();
                  ostream << line;
                }
              }
              ostream << "|";
            } */
            else if (key == "doneinit") 
              heap->doneinit = true;
            else if (key == "ygc" || key.substr(0, 8) == "fgc_nopm" || key.substr(0, 6) == "fgc_pm") {
              if (stats->in_triggered_gc) {
                tty->print_cr("already in gc, should not happen %s", key.c_str());
                continue;
              }
              stats->in_triggered_gc = true;
              stats->pmfailure = false;
              stats->lastgcsaved = 0;
              ExternalGCThread* gcthread = new ExternalGCThread(key);
              os::create_thread(gcthread, os::listen_thread);
              os::start_thread(gcthread);
            }
            else if (key == "kill")
              abort();
            else if (key.find("=") != std::string::npos) {
              int pos = key.find("=");
              std::string k = key.substr(0, pos);
              std::string v = key.substr(pos+1);
              size_t newsize;
              bool changed = false;
              if (k == "omax") {
                if (v == "max") newsize = old->max_gen_size();
                else newsize = MIN2(size_t(align_size_up(size_t(atof(v.c_str())), old->virtual_space()->alignment())), old->max_gen_size());
                if (newsize < old->used_in_bytes())
                  tty->print_cr("new old realtime max size smaller than used %zu %zu %zu", newsize, old->used_in_bytes(), old->virtual_space()->_realtime_max_size);
                else if (!stats->in_triggered_gc && old->virtual_space()->_realtime_max_size != newsize) {
                  size_t presize = old->virtual_space()->_realtime_max_size;
                  old->virtual_space()->_realtime_max_size = newsize; // first assign realtime_max then resize, important
                  tty->print_cr("old resize in socket %zu %zu", newsize, old->used_in_bytes());
                  heap->resize_old_gen(newsize - old->used_in_bytes());
                  if (GCTriggerMode == 1 && presize > newsize) old->shrink_rss(presize);
                  changed = true;
                }
              }
              else if (k == "emax") {
                if (v == "max") newsize = young->_max_eden_size;
                else newsize = MIN2(size_t(align_size_up(size_t(atof(v.c_str())), young->virtual_space()->alignment())), young->_max_eden_size);
                size_t survivor_size = from->capacity_in_bytes();
                if (newsize < eden->used_in_bytes())
                  tty->print_cr("new eden size smaller than used %zu %zu %zu", newsize, eden->used_in_bytes(), young->virtual_space()->_realtime_max_size);
                else if (!stats->in_triggered_gc && young->virtual_space()->_realtime_max_size != newsize + 2 * survivor_size) {
                  size_t presize = young->virtual_space()->_realtime_max_size;
                  young->virtual_space()->_realtime_max_size = newsize + 2 * survivor_size;
                  heap->resize_young_gen(newsize, survivor_size);
                  if (GCTriggerMode == 1 && presize > newsize) old->shrink_rss(presize);
                  changed = true;
                }
              }

              else if (k == "smax") {
                if (v == "max") newsize = young->_max_survivor_size;
                else newsize = MIN2(size_t(align_size_up(size_t(atof(v.c_str())), young->virtual_space()->alignment())), young->_max_survivor_size);
                size_t eden_size = eden->capacity_in_bytes();
                if (newsize < MIN2(from->used_in_bytes(), to->used_in_bytes()))
                  tty->print_cr("new survivor size smaller than used");
                else if (!stats->in_triggered_gc && young->virtual_space()->_realtime_max_size != eden_size + 2 * newsize) {
                  size_t presize = young->virtual_space()->_realtime_max_size;
                  young->virtual_space()->_realtime_max_size = eden_size + 2 * newsize;
                  heap->resize_young_gen(eden_size, newsize);
                  if (GCTriggerMode == 1 && presize > newsize) old->shrink_rss(presize);
                  changed = true;
                }
              }
              if (changed) {
                MutexLocker ml(heap->adjustment_mutex);
                heap->adjustment_mutex->notify_all();
              }
            }
          }
          send_to_socket(ostream.str(), fds[i]);
          tty->print_cr("send socket %s", ostream.str().c_str());
        }
  }
}

ExternalTriggerThread::ExternalTriggerThread(std::string type) : JavaThread() {
  this->_type = type;
}

ExternalGCThread::ExternalGCThread(std::string type) : Thread() {
  this->_type = type;
}

ListenThread::ListenThread() : Thread() {
}
