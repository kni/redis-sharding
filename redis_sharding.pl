$| = 1;
use strict;
use warnings;

use Getopt::Long;
use IO::Socket;
use IO::Select;
use String::CRC32;

use RedisSharding;

my $VERBOSE = 0;


GetOptions(
	"host=s"  => \ my $local_host,
	"port=i"  => \ (my $local_port = 6379),
	"nodes=s" => \ my $nodes,
);


unless ($nodes) {
	print <<EOD;
Parameter 'nodes' is required.

Using example:
perl $0                             --nodes=10.1.1.2:6380,10.1.1.3:6380,10.1.1.4:6380
perl $0                 --port=6379 --nodes=10.1.1.2:6380,10.1.1.3:6380,10.1.1.4:6380
perl $0 --host=10.1.1.1 --port=6379 --nodes=10.1.1.2:6380,10.1.1.3:6380,10.1.1.4:6380
EOD
	exit;
}

my @servers = split /\s*,\s*/, $nodes;

my $blksize = 1024 * 16;

my $sel_r = IO::Select->new();
my $sel_w = IO::Select->new();

my $listener = IO::Socket::INET->new(
	Proto => 'tcp', Blocking => 0,
	LocalHost => $local_host, LocalPort => $local_port,
	Listen => 20, ReuseAddr => 1
) or die $!;

$sel_r->add($listener);





my %cmd_type = ();

$cmd_type{$_} = 1 for qw(PING AUTH SELECT FLUSHDB FLUSHALL DBSIZE KEYS);

$cmd_type{$_} = 2 for qw(
EXISTS TYPE EXPIRE PERSIST TTL MOVE
SET GET GETSET SETNX SETEX
INCR INCRBY DECR DECRBY APPEND SUBSTR
RPUSH LPUSH LLEN LRANGE LTRIM LINDEX LSET LREM LPOP RPOP
SADD SREM SPOP SCARD SISMEMBER SMEMBERS SRANDMEMBER
ZADD ZREM ZINCRBY ZRANK ZREVRANK ZRANGE ZREVRANGE ZRANGEBYSCORE ZCOUNT ZCARD ZSCORE ZREMRANGEBYRANK ZREMRANGEBYSCORE
HSET HGET HMGET HMSET HINCRBY HEXISTS HDEL HLEN HKEYS HVALS HGETALL
);

$cmd_type{$_} = 3 for qw(DEL MGET);   
$cmd_type{$_} = 4 for qw(MSET MSETNX);
$cmd_type{$_} = 5 for qw(BLPOP BRPOP);

my %c2s = ();
my %s2c = ();
my %s2a = ();


my %c2buf = ();
my %s2buf = ();

my %c2client_reader = ();
my %c2servers_reader = ();


sub clean_from_client {
	my ($c) = @_;
	print "clean_from_client\n" if $VERBOSE;
	foreach my $s (values %{$c2s{$c}}) {
		$sel_r->remove($s);
		$sel_w->remove($s);
		delete $s2buf{$s};
		delete $s2c{$s};
		delete $s2a{$s};
		close $s;
	}
	$sel_r->remove($c);
	$sel_w->remove($c);
	delete $c2buf{$c};
	delete $c2client_reader{$c};
	delete $c2servers_reader{$c};
	delete $c2s{$c};
	close $c;
}



sub readers {
	my ($c) = @_;

	my @cmd = ();
	my $cmd;     

	my $client_reader = get_client_reader(
		sub {
			my ($cmd_name, @args) = @_;
			print "REQUEST $cmd_name\n" if $VERBOSE;
			if (my $cmd_type = $cmd_type{$cmd_name}) {
				if ($cmd_type == 1) {
					push @cmd, [$cmd_name];
					my $buf = cmd2stream($cmd_name, @args);
					write2server($_, $buf) for values %{$c2s{$c}};
				} elsif ($cmd_type == 2) {
					my $s_addr = key2server($args[0], \@servers);
					push @cmd, [$cmd_name, $s_addr];
					my $buf = cmd2stream($cmd_name, @args);
					write2server($c2s{$c}{$s_addr}, $buf);
				} elsif ($cmd_type == 3) {
					my @s_addr = ();
					my %s_addr = ();
					for (@args) {
						my $s_addr = key2server($_, \@servers);
						push @s_addr, $s_addr;
						push @{$s_addr{$s_addr}}, $_;
					}
					push @cmd, [$cmd_name, @s_addr];
					foreach my $s_addr (keys %s_addr) {
						my $buf = cmd2stream($cmd_name, @{$s_addr{$s_addr}});
						write2server($c2s{$c}{$s_addr}, $buf);
					}
				} elsif ($cmd_type == 4) {
					my @s_addr = ();
					my %s_addr = ();
					my $i = 0;
					while ($i <= $#args) {
						my $key   = $args[$i++];
						my $value = $args[$i++];
						my $s_addr = key2server($key, \@servers);
						push @s_addr, $s_addr;
						push @{$s_addr{$s_addr}}, $key, $value;
					}
					push @cmd, [$cmd_name, @s_addr];
					foreach my $s_addr (keys %s_addr) {
						my $buf = cmd2stream($cmd_name, @{$s_addr{$s_addr}});
						write2server($c2s{$c}{$s_addr}, $buf);
					}
				} elsif ($cmd_type == 5) {
					my $timeout = $args[-1];
					my @s_addr = ();
					my %s_addr = ();
					for my $i (0 .. $#args - 1) {
						my $s_addr = key2server($args[$i], \@servers);
						push @s_addr, $s_addr;
						push @{$s_addr{$s_addr}}, $args[$i];
					}
					if (keys %s_addr > 1) {
						write2client($c, "-ERR Keys of the '$cmd_name' command should be on one node; use key tags\r\n");
					} else {
						push @cmd, [$cmd_name, @s_addr];
						foreach my $s_addr (keys %s_addr) {
							my $buf = cmd2stream($cmd_name, @{$s_addr{$s_addr}}, $timeout);
							write2server($c2s{$c}{$s_addr}, $buf);
						}
					}
				}
			} else {
				write2client($c, "-ERR unsupported command '$cmd_name'\r\n");
			}
		}
	);

	my $resp_type = "";
	my %resp_line      = ();
	my %resp_bulk_size = ();
	my %resp_bulk_args = ();
	my $servers_reader = get_servers_reader(
		DEBUG   => 0,
		servers => \@servers,
		sub_cmd => sub {
			if ($cmd = shift @cmd) {
				my ($cmd_name, @s_addr) = @$cmd;
				if (@s_addr) {
					my %s_addr = map { $_ => 1 } @s_addr;
					return $cmd_name, keys %s_addr;
				} else {
					return $cmd_name;
				}
			}
		},
		sub_response_type          => sub { ($resp_type) = @_ },
		sub_line_response          => sub { my ($s, $resp_line) = @_; $resp_line{$s} = $resp_line },
		sub_bulk_response_size     => sub { my ($s, $resp_bulk_size) = @_; $resp_bulk_size{$s} = $resp_bulk_size },
		sub_bulk_response_size_all => sub { },
		sub_bulk_response_arg      => sub { my ($s, $arg) = @_; push @{$resp_bulk_args{$s}}, $arg },
		sub_response_received      => sub {
			if ($resp_type eq "line") {
				my @v = values %resp_line;
				if (@v == 1) {
					write2client($c, "$v[0]\r\n");
				} else {
					my $v = shift @v;
					if ($v =~ m/^:\d+/ ) {
						my $sum = 0;
						$sum += $_ for map { m/^:(\d+)/ } $v, @v;
						write2client($c, ":$sum\r\n");
					} else {
						if (grep { $v ne $_ } @v) {
							write2client($c, "-ERR nodes return different results\r\n");
						} else {
							write2client($c, "$v\r\n");
						}
					}
				}
			} elsif ($resp_type eq "bulk") {

				if ($cmd_type{$$cmd[0]} != 2) {
					warn "bulk cmd $$cmd[0] with $cmd_type{$$cmd[0]} != 2";
				}

				if (keys %resp_bulk_size > 1) {
					warn "logic error";
				} else {
					my $s_addr = (keys %resp_bulk_size)[0];
					if (defined $resp_bulk_size{$s_addr}) {
						my $arg = $resp_bulk_args{$s_addr}[0];
						my $stream = join "", '$', length $arg, "\r\n", $arg, "\r\n";
						write2client($c, $stream);
					} else {
						write2client($c, "\$-1\r\n");
					}
				}

			} elsif ($resp_type eq "multi_bulk") {
				my $resp_bulk_size;
				$resp_bulk_size += $_ for grep { defined $_ } values %resp_bulk_size;
				if (defined $resp_bulk_size) {
					my ($cmd_name, @s_addr) = @$cmd;
					my @args = ();
					if (@s_addr) {
						foreach my $s_addr (@s_addr) {
							push @args, shift @{$resp_bulk_args{$s_addr}};
						}
					} else {
						foreach (values %resp_bulk_args) {
							push @args, @$_;
						}
					}
				 	write2client($c, cmd2stream(@args));
				} else {
					write2client($c, "*-1\r\n");
				}
			}
			print "RESPONSE from all on $$cmd[0]\n" if $VERBOSE;
			$resp_type = "";
			%resp_line      = ();
			%resp_bulk_size = ();
			%resp_bulk_args = ();
		},
	);

	return $client_reader, $servers_reader;
}



sub write2client {
	my ($c, $buf) = @_;
	$c2buf{$c} .= $buf;
	$sel_w->add($c) unless $sel_w->exists($c);
}

sub write2server {
	my ($s, $buf) = @_;
	$s2buf{$s} .= $buf;
	$sel_w->add($s) unless $sel_w->exists($s);
}


sub key2server {
	my ($key, $servers) = @_;
	$$servers[crc32($key =~ m/{(.+)}$/ ? $1 : $key) % @$servers];
}



RESET: while ($sel_r->count()) {
	my ($can_read, $can_write, $has_exception)= IO::Select->select($sel_r, $sel_w);

	foreach my $fh (@$can_read) {
		if ($fh eq $listener) {
			my $c_sock = $listener->accept;
			$c_sock->sockopt(SO_KEEPALIVE, 1);
			$sel_r->add($c_sock);
			foreach (@servers) {
				my ($host, $port) = split /:/;
				my $s_sock = IO::Socket::INET->new(Proto => 'tcp', Blocking => 0, PeerHost => $host, PeerPort => $port);
				unless ($s_sock) {
					warn $!;
					clean_from_client($c_sock);
					next RESET;
				}
				$s_sock->sockopt(SO_KEEPALIVE, 1);
				$c2s{$c_sock}{$_} = $s_sock;
				$s2c{$s_sock} = $c_sock;
				$s2a{$s_sock} = $_;
				$sel_r->add($s_sock);
			}
			($c2client_reader{$c_sock}, $c2servers_reader{$c_sock}) = readers($c_sock);
		} elsif ($c2s{$fh}) {
			my $len = sysread $fh, (my $buf), $blksize;
			if ($len) {
				my $rv = $c2client_reader{$fh}->($buf);
				unless ($rv) {
					warn "ERROR: unified protocol error";
					write2client($fh, "-ERR unified protocol error\r\n");
				}
			} elsif (defined $len) {
				clean_from_client($fh);
				next RESET;
			}
		} elsif (my $c = $s2c{$fh}) {
			my $len = sysread $fh, (my $buf), $blksize;
			if ($len) {
				$c2servers_reader{$c}->($s2a{$fh}, $buf);
			} elsif (defined $len) {
				clean_from_client($c);
				next RESET;
			}
		}
	}
	foreach my $fh (@$can_write) {
		if ($s2c{$fh}) {
			my $buf = $s2buf{$fh};
			my $len = syswrite $fh, $buf, $blksize;
			if ($len) {
				substr $buf, 0, $len, "";
				$s2buf{$fh} = $buf;
				unless (length $buf) {
					$sel_w->remove($fh);
				}
			}		
		} elsif ($c2s{$fh}) {
			my $buf = $c2buf{$fh};
			my $len = syswrite $fh, $buf, $blksize;
			if ($len) {
				substr $buf, 0, $len, "";
				$c2buf{$fh} = $buf;
				unless (length $buf) {
					$sel_w->remove($fh);
				}
			}		
		}
	}
}


