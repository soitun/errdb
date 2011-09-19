# encoding: utf-8

require 'socket'

require 'net/protocol'

require 'errdb/version.rb'

begin
  # Try to use the SystemTimer gem instead of Ruby's timeout library
  # when running on Ruby 1.8.x. See:
  #   http://ph7spot.com/articles/system_timer
  # We don't want to bother trying to load SystemTimer on jruby,
  # ruby 1.9+ and rbx.
  if !defined?(RUBY_ENGINE) || (RUBY_ENGINE == 'ruby' && RUBY_VERSION < '1.9.0')
    require 'system_timer'
    ErrdbTimer = SystemTimer
  else
    require 'timeout'
    ErrdbTimer = Timeout
  end
rescue LoadError => e
  require 'timeout'
  ErrdbTimer = Timeout
end

##
# A Ruby client library for errdb.
#

class Errdb

  ##
  # Default options for the cache object.

  DEFAULT_OPTIONS = {
    :readonly     => true,
    :timeout      => 0.5,
    :logger       => nil,
    :no_reply     => false,
    :check_size   => true,
  }

  ##
  # Default errdb port.

  DEFAULT_PORT = 7272

  ##
  # The server this client talks to.  Play at your own peril.

  attr_reader :host

  attr_reader :port

  attr_reader :server

  ##
  # Socket timeout limit with this client, defaults to 0.5 sec.
  # Set to nil to disable timeouts.

  attr_reader :timeout

  ##

  ##
  # Log debug/info/warn/error to the given Logger, defaults to nil.

  attr_reader :logger

  ##
  # Accepts a list of +servers+ and a list of +opts+.  +servers+ may be
  # omitted.  See +servers=+ for acceptable server list arguments.
  #
  # Valid options for +opts+ are:
  #
  #   [:namespace]    Prepends this value to all keys added or retrieved.
  #   [:readonly]     Raises an exception on cache writes when true.
  #   [:timeout]      Time to use as the socket read timeout.  Defaults to 0.5 sec,
  #                   set to nil to disable timeouts.
  #   [:logger]       Logger to use for info/debug output, defaults to nil
  #   [:no_reply]     Don't bother looking for a reply for write operations (i.e. they
  #                   become 'fire and forget'), errdb 1.2.5 and later only, speeds up
  #                   set/add/delete/incr/decr significantly.
  #   [:check_size]   Raises a ErrdbError if the value to be set is greater than 1 MB, which
  #                   is the maximum key size for the standard errdb server.  Defaults to true.
  #   [:autofix_keys] If a key is longer than 250 characters or contains spaces,
  #                   use an SHA1 hash instead, to prevent collisions on truncated keys.
  # Other options are ignored.

  def initialize(options = {})
    @host = options[:host] || "127.0.0.1"
    @port = (options[:port] || DEFAULT_PORT).to_i
    @readonly     = options[:readonly] || true
    @timeout = (options[:timeout] || 5).to_f
    @logger       = options[:logger]
    @server = Server.new(self, @host, @port)
    logger.info { "errdb-client #{VERSION} #{server}" } if logger
  end

  ##
  # Returns a string representation of the cache object.

  def inspect
    "<Errdb: %s , ro: %p>" %
      [@server.inspect, @readonly]
  end

  ##
  # Returns whether there is at least one active server for the object.

  def active?
    @server.active?
  end

  ##
  # Returns whether or not the cache object was created read only.

  def readonly?
    @readonly
  end

  ##
  # Retrieves +key+ from errdb.  If +raw+ is false, the value will be
  # unmarshalled.


  #time:a,b,c" =~ /\Atime: (.*)/

  #"19199191:a,b,c" =~ /\A(\d+):(.*)/


  def fetch(key, start_time, end_time)
    rows = {}
    names = []
    values = []
    with_socket(server) do |socket|
      socket.write "fetch #{key} #{start_time} #{end_time}\r\n"
      while line = socket.gets do
        raise_on_error_response! line
        break if line == "END\r\n"
        if line =~ /\Atime: (.*)/ then
          names = $1.strip.split(",")
        elsif line =~ /\A(\d+):(.*)/ then
          timestamp = $1
          values = $2.strip.split(",")
          row = {}
          names.each_index {|i| row[names[i]] = values[i].to_f }
          rows[timestamp.to_i] = row
        else
          logger.warn { "unexpected line: #{line}" } if logger
        end
      end
    end
    rows
  end

  ##
  # Gets or creates a socket connected to the given server, and yields it
  # to the block
  #
  # If a socket error (SocketError, SystemCallError, IOError) or protocol error
  # (ErrdbError) is raised by the block, closes the socket, attempts to
  # connect again, and retries the block (once).  If an error is again raised,
  # reraises it as ErrdbError.
  #
  # If unable to connect to the server (or if in the reconnect wait period),
  # raises ErrdbError.  Note that the socket connect code marks a server
  # dead for a timeout period, so retrying does not apply to connection attempt
  # failures (but does still apply to unexpectedly lost connections etc.).

  def with_socket(server, &block)

    begin
      socket = server.socket

      # Raise an IndexError to show this server is out of whack. If were inside
      # a with_server block, we'll catch it and attempt to restart the operation.

      raise IndexError, "No connection to server (#{server.status})" if socket.nil?

      block.call(socket)

    rescue SocketError, Errno::EAGAIN, Timeout::Error => err
      logger.warn { "Socket failure: #{err.message}" } if logger
      server.mark_dead(err)
      handle_error(server, err)

    rescue ErrdbError, SystemCallError, IOError => err
      logger.warn { "Generic failure: #{err.class.name}: #{err.message}" } if logger
      handle_error(server, err) if socket.nil?
    end

  end

  ##
  # Handles +error+ from +server+.

  def handle_error(server, error)
    raise error if error.is_a?(ErrdbError)
    server.close if server && server.status == "CONNECTED"
    new_error = ErrdbError.new error.message
    new_error.set_backtrace error.backtrace
    raise new_error
  end

  def raise_on_error_response!(response)
    if response =~ /\A(?:CLIENT_|SERVER_)?ERROR(.*)/
      raise ErrdbError, $1.strip
    end
  end

  class Server

    ##
    # The amount of time to wait before attempting to re-establish a
    # connection with a server that is marked dead.

    RETRY_DELAY = 30.0

    ##
    # The host the errdb server is running on.

    attr_reader :host

    ##
    # The port the errdb server is listening on.

    attr_reader :port

    ##
    # The time of next retry if the connection is dead.

    attr_reader :retry

    ##
    # A text status string describing the state of the server.

    attr_reader :status

    attr_reader :logger

    ##
    # Create a new Errdb::Server object for the errdb instance
    # listening on the given host and port

    def initialize(errdb, host, port = DEFAULT_PORT)
      raise ArgumentError, "No host specified" if host.nil? or host.empty?
      raise ArgumentError, "No port specified" if port.nil? or port.to_i.zero?

      @host   = host
      @port   = port.to_i

      @sock   = nil
      @retry  = nil
      @status = 'NOT CONNECTED'
      @timeout = errdb.timeout
      @logger = errdb.logger
    end

    ##
    # Return a string representation of the server object.

    def inspect
      "<Errdb::Server: %s:%d (%s)>" % [@host, @port, @status]
    end

    ##
    # Check whether the server connection is alive.  This will cause the
    # socket to attempt to connect if it isn't already connected and or if
    # the server was previously marked as down and the retry time has
    # been exceeded.

    def alive?
      !!socket
    end

    ##
    # Try to connect to the errdb server targeted by this object.
    # Returns the connected socket object on success or nil on failure.

    def socket
      return @sock if @sock and not @sock.closed?

      @sock = nil

      # If the host was dead, don't retry for a while.
      return if @retry and @retry > Time.now

      # Attempt to connect if not already connected.
      begin
        @sock = connect_to(@host, @port, @timeout)
        @sock.setsockopt Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1
        @retry  = nil
        @status = 'CONNECTED'
      rescue SocketError, SystemCallError, IOError, Timeout::Error => err
        logger.warn { "Unable to open socket: #{err.class.name}, #{err.message}" } if logger
        mark_dead err
      end

      return @sock
    end

    def connect_to(host, port, timeout=nil)
      sock = nil
      if timeout
        ErrdbTimer.timeout(timeout) do
          sock = TCPSocket.new(host, port)
        end
      else
        sock = TCPSocket.new(host, port)
      end

      io = Errdb::BufferedIO.new(sock)
      io.read_timeout = timeout
      # Getting reports from several customers, including 37signals,
      # that the non-blocking timeouts in 1.7.5 don't seem to be reliable.
      # It can't hurt to set the underlying socket timeout also, if possible.
      if timeout
        secs = Integer(timeout)
        usecs = Integer((timeout - secs) * 1_000_000)
        optval = [secs, usecs].pack("l_2")
        begin
          io.setsockopt Socket::SOL_SOCKET, Socket::SO_RCVTIMEO, optval
          io.setsockopt Socket::SOL_SOCKET, Socket::SO_SNDTIMEO, optval
        rescue Exception => ex
          # Solaris, for one, does not like/support socket timeouts.
          @logger.info "[errdb-client] Unable to use raw socket timeouts: #{ex.class.name}: #{ex.message}" if @logger
        end
      end
      io
    end

    ##
    # Close the connection to the errdb server targeted by this
    # object.  The server is not considered dead.

    def close
      @sock.close if @sock && !@sock.closed?
      @sock   = nil
      @retry  = nil
      @status = "NOT CONNECTED"
    end

    ##
    # Mark the server as dead and close its socket.

    def mark_dead(error)
      close
      @retry  = Time.now + RETRY_DELAY

      reason = "#{error.class.name}: #{error.message}"
      @status = sprintf "%s:%s DEAD (%s), will retry at %s", @host, @port, reason, @retry
      @logger.info { @status } if @logger
    end

  end

  ##
  # Base Errdb exception class.

  class ErrdbError < RuntimeError; end

  class BufferedIO < Net::BufferedIO # :nodoc:
    BUFSIZE = 1024 * 16

    if RUBY_VERSION < '1.9.1'
      def rbuf_fill
        begin
          @rbuf << @io.read_nonblock(BUFSIZE)
        rescue Errno::EWOULDBLOCK
          retry unless @read_timeout
          if IO.select([@io], nil, nil, @read_timeout)
            retry
          else
            raise Timeout::Error, 'IO timeout'
          end
        end
      end
    end

    def setsockopt(*args)
      @io.setsockopt(*args)
    end

    def gets
      encode(readuntil("\n"))
    end

    def encode(str)
       str
    end
  end

end

