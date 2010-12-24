
package RedisSharding;

use strict;
use warnings;

use String::CRC32;

use Exporter;
our @ISA = qw(Exporter);
our @EXPORT = qw(cmd2stream get_client_reader get_servers_reader readers);







sub get_client_reader {
	my ($sub_cmd, $DEBUG) = @_;

	my $buf           = "";
	my $i             = 0;
	my $args_cnt      = 0;
	my $size_next_arg = 0;
	my @args          = ();

	my $sub_clean = sub {
		$i             = 0;
		$args_cnt      = 0;
		$size_next_arg = 0;
		@args          = ();
	};

	return sub {
		my ($_buf) = @_;
		$buf .= $_buf;
		NEXT: {
			if (not $args_cnt) {
				$buf =~ s/^(\r\n)+//;
				return 1 unless $buf;
				if ((index $buf, '*', 0) == 0) {
					if ((my $_i = index $buf, "\r\n", 1) > -1) {
						$args_cnt = substr $buf, 1, $_i - 1;
						print "args_cnt: $args_cnt\n" if $DEBUG;
						$i = $_i + 2;
					}
				} else {
					print "PARSING ERROR: args_cnt\n" if $DEBUG;
					$buf = ""; $sub_clean->();
					return;
				}
			}
			return 1 unless length $buf > $i;
			if ($args_cnt and not $size_next_arg) {
				if ((index $buf, '$', $i) == $i) {
					if ((my $_i = index $buf, "\r\n", $i + 1) > -1) {
						$size_next_arg = substr $buf, $i + 1, $_i - ($i + 1);
						print "size_next_arg: $size_next_arg\n" if $DEBUG;
						$i = $_i + 2;
					}
				} else {
					print "PARSING ERROR: size_next_arg\n" if $DEBUG;
					$buf = ""; $sub_clean->();
					return;
				}
			}
			return 1 unless length $buf > $i;
			if ($args_cnt and $size_next_arg) {
				if ((index $buf, "\r\n", $i + $size_next_arg) == $i + $size_next_arg) {
					my $arg = substr $buf, $i, $size_next_arg;
					print "arg: $arg\n" if $DEBUG;
					push @args, $arg;
					$i = $i + $size_next_arg + 2;
					$size_next_arg = 0;
					if ($args_cnt == @args) {
						print "NEXT COMMAND\n" if $DEBUG;
						$sub_cmd->(@args);
						substr $buf, 0, $i, "";
						$sub_clean->();
					}
					redo NEXT if length $buf;
				} elsif (length $buf > $i + $size_next_arg + 2) {
					print "PARSING ERROR: arg\n" if $DEBUG;
					$buf = ""; $sub_clean->();
					return;
				}
			}
		}

		return 1;
	};
}






sub get_servers_reader {
	my %args = @_;

	my %buf = ();

	my ($cmd, @s);

	my %cmd = ();

	my $reply_type = "";

	my %bulk_args     = ();
	my %bulk_args_had = ();
	my %bulk_size     = ();
	my $size_all      = 0; 
	
	my $sub_clean = sub {
		($cmd, @s) = ();
		%cmd = ();
		$reply_type = "";
		%bulk_args     = ();
		%bulk_args_had = ();
		%bulk_size     = ();
		$size_all      = 0;
	};


	return sub {
		my ($s, $buf) = @_;
		$buf{$s} .= $buf;

		my $next;
		NEXT: {
			$next = 0;
			unless ($cmd) {
				($cmd, @s) = $args{sub_cmd}->();
				@s or @s = @{$args{servers}};
			}

			my $re_line   = qr/([+]\w+?|:\d+)\r\n/;
			my $re_bulk_1 = qr/\*(\d+|-1)\r\n/;
			my $re_bulk_2 = qr/\$(\d+|-1)\r\n/;

			foreach my $s (@s) {
				unless ($cmd{$s}) {
					if ($buf{$s}) {
						if ($reply_type =~ m/^(|line)$/ and $buf{$s} =~ m/^$re_line/) {
							print "RESPONSE from $s on $cmd: $1\n" if $args{DEBUG};
							$reply_type or $args{sub_response_type}->("line");
							$reply_type ||= "line";
							$args{sub_line_response}->($s, $1);
							$cmd{$s} = $1;
							substr $buf{$s}, 0, $+[0], "";
							$next = 1;
						} elsif ($reply_type =~ m/^(|multi_bulk)$/ and not exists $bulk_args{$s} and $buf{$s} =~ m/^$re_bulk_1/) {
							print "RESPONSE from $s on $cmd: *$1}\\r\\n\n" if $args{DEBUG};
							$reply_type or $args{sub_response_type}->("multi_bulk");
							$reply_type ||= "multi_bulk";
							if ($1 == -1) {
								$bulk_args{$s} = undef;
								$args{sub_bulk_response_size}->($s, undef);
								$bulk_args_had{$s}++;
								$cmd{$s} = 1;
							} elsif ($1 == 0) {
								$bulk_args{$s} = 0;
								$args{sub_bulk_response_size}->($s, 0);
								$bulk_args_had{$s}++;
								$cmd{$s} = 1;
							} else {
								$bulk_args{$s} = $1;
								$args{sub_bulk_response_size}->($s, $1);
							}
							substr $buf{$s}, 0, $+[0], "";
							$next = 1;
						} elsif ($reply_type =~ m/^(|bulk)$/ and not exists $bulk_args{$s} and $buf{$s} =~ m/^$re_bulk_2/) {
							print "RESPONSE from $s on $cmd: \$$1}\\r\\n\n" if $args{DEBUG};
							$reply_type or $args{sub_response_type}->("bulk");
							$reply_type ||= "bulk";
							if ($1 == -1) {
								$bulk_args{$s} = undef;
								$args{sub_bulk_response_size}->($s, undef);
								$bulk_args_had{$s}++;
								$cmd{$s} = 1;
							} else {
								$bulk_args{$s} = 1;
								$bulk_size{$s} = $1;
								$args{sub_bulk_response_size}->($s, 1);
							}
							substr $buf{$s}, 0, $+[0], "";
							$next = 1;
						} elsif ($reply_type =~ m/^multi_bulk$/ and not exists $bulk_size{$s} and $buf{$s} =~ m/^$re_bulk_2/) {
							if ($1 == -1) {
								$args{sub_bulk_response_arg}->($s, undef);
								$bulk_args_had{$s}++;
								$cmd{$s} = 1 if $bulk_args{$s} == $bulk_args_had{$s};
							} else {
								$bulk_size{$s} = $1;
							}
							print "RESPONSE from $s on $cmd: \$", $bulk_size{$s} // "-1", "\\r\\n\n" if $args{DEBUG};
							substr $buf{$s}, 0, $+[0], "";
							$next = 1;
						} elsif ($reply_type =~ m/^(multi_)?bulk$/ and defined $bulk_size{$s}) {
							if ($buf{$s} =~ m/^(.{$bulk_size{$s}})\r\n/s) {
								print "RESPONSE from $s on $cmd: $1\\r\\n\n" if $args{DEBUG};
								$args{sub_bulk_response_arg}->($s, $1);
								$bulk_args_had{$s}++;
								$cmd{$s} = 1 if $bulk_args{$s} == $bulk_args_had{$s};
								delete $bulk_size{$s};
								substr $buf{$s}, 0, $+[0], "";
								$next = 1;
							} elsif (length $buf{$s} >= $bulk_size{$s} + 2) {
								warn "PARSING ERROR for $cmd";
							}
						}
					}
				}
			}
 
			if (not $size_all and $reply_type =~ m/^(multi_)?bulk$/ and @s eq keys %bulk_args) {
				$args{sub_bulk_response_size_all}->();
				$size_all = 1;
			}
			if (@s eq keys %cmd) {
				print "RESPONSE from all on $cmd\n" if $args{DEBUG};
				$args{sub_response_received}->();
				$sub_clean->();
				$next = 1;
			}

			redo NEXT if $next;
		}
	};
}





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



sub readers {
	my ($c, $servers, $write2server, $write2client, $VERBOSE) = @_;

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
					$write2server->($c, undef, $buf);
				} elsif ($cmd_type == 2) {
					my $s_addr = key2server($args[0], $servers);
					push @cmd, [$cmd_name, $s_addr];
					my $buf = cmd2stream($cmd_name, @args);
					$write2server->($c, $s_addr, $buf);
				} elsif ($cmd_type == 3) {
					my @s_addr = ();
					my %s_addr = ();
					for (@args) {
						my $s_addr = key2server($_, $servers);
						push @s_addr, $s_addr;
						push @{$s_addr{$s_addr}}, $_;
					}
					push @cmd, [$cmd_name, @s_addr];
					foreach my $s_addr (keys %s_addr) {
						my $buf = cmd2stream($cmd_name, @{$s_addr{$s_addr}});
						$write2server->($c, $s_addr, $buf);
					}
				} elsif ($cmd_type == 4) {
					my @s_addr = ();
					my %s_addr = ();
					my $i = 0;
					while ($i <= $#args) {
						my $key   = $args[$i++];
						my $value = $args[$i++];
						my $s_addr = key2server($key, $servers);
						push @s_addr, $s_addr;
						push @{$s_addr{$s_addr}}, $key, $value;
					}
					push @cmd, [$cmd_name, @s_addr];
					foreach my $s_addr (keys %s_addr) {
						my $buf = cmd2stream($cmd_name, @{$s_addr{$s_addr}});
						$write2server->($c, $s_addr, $buf);
					}
				} elsif ($cmd_type == 5) {
					my $timeout = $args[-1];
					my @s_addr = ();
					my %s_addr = ();
					for my $i (0 .. $#args - 1) {
						my $s_addr = key2server($args[$i], $servers);
						push @s_addr, $s_addr;
						push @{$s_addr{$s_addr}}, $args[$i];
					}
					if (keys %s_addr > 1) {
						$write2client->($c, "-ERR Keys of the '$cmd_name' command should be on one node; use key tags\r\n");
					} else {
						push @cmd, [$cmd_name, @s_addr];
						foreach my $s_addr (keys %s_addr) {
							my $buf = cmd2stream($cmd_name, @{$s_addr{$s_addr}}, $timeout);
							$write2server->($c, $s_addr, $buf);
						}
					}
				}
			} else {
				$write2client->($c, "-ERR unsupported command '$cmd_name'\r\n");
			}
		}
	);

	my $resp_type = "";
	my %resp_line      = ();
	my %resp_bulk_size = ();
	my %resp_bulk_args = ();
	my $servers_reader = get_servers_reader(
		DEBUG   => 0,
		servers => $servers,
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
		sub_bulk_response_size_all => sub {
			if ($$cmd[0] eq "KEYS") {
				my $resp_bulk_size;
				$resp_bulk_size += $_ for grep { defined $_ } values %resp_bulk_size;
				if (defined $resp_bulk_size) {
				 	$write2client->($c, "*$resp_bulk_size\r\n");
				} else {
					$write2client->($c, "*-1\r\n");
				}
			}
		},
		sub_bulk_response_arg      => sub {
			my ($s, $arg) = @_;
			if ($$cmd[0] eq "KEYS") {
				if (defined $arg) {
					$write2client->($c, join "", '$', length $arg, "\r\n", $arg, "\r\n");
				} else {
					$write2client->($c, "\$-1\r\n");
				}
			} else {
				push @{$resp_bulk_args{$s}}, $arg;
			}
		},
		sub_response_received      => sub {
			if ($resp_type eq "line") {
				my @v = values %resp_line;
				if (@v == 1) {
					$write2client->($c, "$v[0]\r\n");
				} else {
					my $v = shift @v;
					if ($v =~ m/^:\d+/ ) {
						my $sum = 0;
						$sum += $_ for map { m/^:(\d+)/ } $v, @v;
						$write2client->($c, ":$sum\r\n");
					} else {
						if (grep { $v ne $_ } @v) {
							$write2client->($c, "-ERR nodes return different results\r\n");
						} else {
							$write2client->($c, "$v\r\n");
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
						$write2client->($c, $stream);
					} else {
						$write2client->($c, "\$-1\r\n");
					}
				}

			} elsif ($resp_type eq "multi_bulk" and $$cmd[0] ne "KEYS") {
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
				 	$write2client->($c, cmd2stream(@args));
				} else {
					$write2client->($c, "*-1\r\n");
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



sub key2server {
	my ($key, $servers) = @_;
	$$servers[crc32($key =~ m/{(.+)}$/ ? $1 : $key) % @$servers];
}



sub cmd2stream {
	join "", 
		'*', scalar @_, "\r\n",
		map {
			if (defined $_) {
				('$', length $_, "\r\n", $_, "\r\n");
			} else {
				('$-1', "\r\n");
			}
		} @_;
}



1;
