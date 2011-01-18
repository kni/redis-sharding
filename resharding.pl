$| = 1;
use strict;
use warnings;

use RedisSharding;

use Getopt::Long;
use IO::Socket;
use EV;


GetOptions(
	"db=i"    => \ my $db,
	"from=s"  => \ my $from,
	"nodes=s" => \ my $nodes,
	"flushdb" => \ my $flushdb,
);

unless (defined $db and $from and $nodes) {
	print <<EOD;
Parameters 'db', 'from' and 'nodes' is required.

Using example:
perl $0 --db=9 --from=10.1.1.1:6379 --nodes=10.1.1.2:6380,10.1.1.3:6380,...
EOD
	exit;
}

my $blksize = 1024 * 16;

sub connecter {
	my ($host, $port) = split /:/, $_[0];
	IO::Socket::INET->new(Proto => 'tcp', Blocking => 0, PeerHost => $host, PeerPort => $port) or die $!;
}


my $fh_keys = connecter($from);
my $fh_type = connecter($from);

my %addr2fh = map { $_ => connecter($_) } split /\s*,\s*/, $nodes;

my %fh2rw = ();
my %fh2ww = ();


my %fh2buf = ();
my %fh2servers_reader = ();
my %fh2nodes_send_cmd = ();

my %fh2name = (
	$fh_keys => "fh_keys",
	$fh_type => "fh_type",
);
foreach (keys %addr2fh) {
	$fh2name{$addr2fh{$_}} = $_;
}

sub w_event_cb {
	my $w = shift;
	my $fh = $w->fh;
	my $len = syswrite $fh, $fh2buf{$fh}, $blksize;
	if ($len) {
		substr $fh2buf{$fh}, 0, $len, "";
		unless (length $fh2buf{$fh}) {
			delete $fh2ww{$fh};
		}
	}
}

sub r_event_cb {
	my $w = shift;
	my $fh = $w->fh;
	my $len = sysread $fh, (my $buf), $blksize;
	if ($len) {
		$fh2servers_reader{$fh}->($fh2name{$fh}, $buf);
	} elsif (defined $len) {
		delete $fh2rw{$fh};
		delete $fh2ww{$fh};
		close $fh;
		warn "ERROR: Redis server '$fh2name{$fh}' close connection";
		exit;
	}
};


my $progress = 0;
my $flag_all_keys;

my $counter = 0;
my $fh_keys_w_stoped = 0;
my $counter_keys = 0;
my $counter_type = 0;
my $counter_node_query = 0;
my $counter_node_resp  = 0;

sub counter_incr {
	$counter++;
	if (not $fh_keys_w_stoped and $counter > 100) {
		$fh_keys_w_stoped = 1;
		$fh2rw{$fh_keys}->stop();
	}
}
sub counter_decr {
	$counter--;
	if ($fh_keys_w_stoped and $counter < 50) {
		$fh_keys_w_stoped = 0;
		$fh2rw{$fh_keys}->start();
	}
	if ($flag_all_keys and $counter == 0) {
		unless (grep { $_ } map { length $_ } values %fh2buf) {
			print "\nDONE ($progress).\n";
			counter_show();
			exit;
		}
	}
}

sub counter_show {
	print "counter_keys $counter_keys\n";
	print "counter_type $counter_type\n";
	print "counter_node_query $counter_node_query\n";
	print "counter_node_resp  $counter_node_resp\n";
}


{
	my @cmd = ();
	my @allcmd = ();

	my $send_cmd = sub {
		if (not $fh_keys_w_stoped) {
		if (my $next_cmd = shift @allcmd) {
			$fh2buf{$fh_keys} .= cmd2stream(@$next_cmd);
			push @cmd, [$$next_cmd[0]];
			$fh2ww{$fh_keys} ||= EV::io($fh_keys, EV::WRITE, \&w_event_cb);
		}
		}
	};

	sub fh_keys_send_cmd {
		push @allcmd, \@_;
		$fh2buf{$fh_keys} or $send_cmd->();
	}

	my $cmd;
	$fh2servers_reader{$fh_keys} = get_servers_reader(
		servers => ["fh_keys"],
		sub_cmd                    => sub {
			if ($cmd = shift @cmd) {
				@$cmd;
			}
		},
		sub_response_type          => sub { my ($type) = @_ },
		sub_line_response          => sub { my ($s, $v) = @_;
			$v =~ m/^-ERR/ and die "ERROR on $$cmd[0]: ", $v;
		},
		sub_bulk_response_size     => sub { my ($s, $v) = @_;
			if ($$cmd[0] eq "KEYS") {
				print "DB $db contain $v keys.\n";
				unless ($v) {
					print "DONE.\n";
					exit;
				}
			} else {
				$v //= "-1"; print "RESPONSE from $s on $$cmd[0]; bulk size: $v\n"
			}
		},
		sub_bulk_response_size_all => sub { },
		sub_bulk_response_arg      => sub {
			my ($s, $v) = @_;
			if ($$cmd[0] eq "KEYS") {
				if (defined $v) {
					type_req($v);
					counter_incr();
					$counter_keys++;
				}
			} else {
				$v //= "-1";
				print "$s: $$cmd[0]: $v\n";
			}
		},
		sub_response_received      => sub {
			$send_cmd->();
			if ($$cmd[0] eq "KEYS") {
				$flag_all_keys = 1;
			}
		},
		DEBUG => 0,
	);

	$fh2rw{$fh_keys} = EV::io($fh_keys, EV::READ, \&r_event_cb);
}



{
	my @cmd = ();
	my @allcmd = ();

	my $send_cmd = sub {
		if (my $next_cmd = shift @allcmd) {
			$fh2buf{$fh_type} .= cmd2stream(@$next_cmd);
			push @cmd, [$$next_cmd[0]];
			$fh2ww{$fh_type} ||= EV::io($fh_type, EV::WRITE, \&w_event_cb);
		}
	};

	sub fh_type_send_cmd {
		push @allcmd, \@_;
		$fh2buf{$fh_type} or $send_cmd->();
	}

	my @k = ();
	sub type_req {
		my ($k) = @_;
		push @k, $k;
		fh_type_send_cmd("TYPE", $k);
	}

	my @kv = ();
	my @v  = ();

	my $DEBUG = 0;
	my $cmd;
	$fh2servers_reader{$fh_type} = get_servers_reader(
		servers => ["fh_type"],
		sub_cmd                    => sub {
			if ($cmd = shift @cmd) {
				@$cmd;
			}
		},
		sub_response_type          => sub { my ($type) = @_ },
		sub_line_response          => sub { my ($s, $v) = @_;
			$v =~ m/^-ERR/ and die "ERROR on $$cmd[0]: ", $v;
			if ($$cmd[0] eq "TYPE") {
				my $k = shift @k;
				print "$s: $$cmd[0]: $k is $v\n" if $DEBUG;
				push @kv, $k;
				if ($v eq "+string") {
					fh_type_send_cmd("GET", $k);
				} elsif ($v eq "+list") {
					fh_type_send_cmd("LRANGE", $k, 0, -1);
				} elsif ($v eq "+set") {
					fh_type_send_cmd("SMEMBERS", $k);
				} elsif ($v eq "+zset") {
					fh_type_send_cmd("ZRANGE", $k, 0, -1, "WITHSCORES");
				} elsif ($v eq "+hash") {
					fh_type_send_cmd("HGETALL", $k);
				} else {
					counter_decr();
					$counter_keys--;
				}
			}
		},
		sub_bulk_response_size     => sub { },
		sub_bulk_response_size_all => sub { },
		sub_bulk_response_arg      => sub { my ($s, $v) = @_; push @v, $v },
		sub_response_received      => sub {
			my %to_nodes = (
				GET => sub {
					my ($addr, $k, @v) = @_;
					fh_nodes_send_cmd($addr, "SET", $k, @v);
				},
				LRANGE => sub {
					my ($addr, $k, @v) = @_;
					foreach my $v (@v) {
						fh_nodes_send_cmd($addr, "RPUSH", $k, $v);
					}
				},
				SMEMBERS => sub {
					my ($addr, $k, @v) = @_;
					foreach my $v (@v) {
						fh_nodes_send_cmd($addr, "SADD", $k, $v);
					}
				},
				ZRANGE => sub {
					my ($addr, $k, @v) = @_; 
					my $i = 0;
					while ($i <= $#v) {
						my $v = $v[$i++];
						my $s = $v[$i++];
						fh_nodes_send_cmd($addr, "ZADD", $k, $s, $v);
					}
				},
				HGETALL => sub {
					my ($addr, $k, @v) = @_;
					fh_nodes_send_cmd($addr, "HMSET", $k, @v);
				},
			);
			if (my $to_nodes = $to_nodes{$$cmd[0]}) {
			 	my $k = shift @kv;
			 	my $addr = RedisSharding::key2server($k, [keys %addr2fh]);
				if ($DEBUG) {
					require Data::Dumper;
				 	print Data::Dumper::Dumper([$$cmd[0], $k, @v]);
				}
				$to_nodes->($addr, $k, @v) if @v;
				print "." unless ++$progress % 1000;
				counter_decr();
				$counter_type++;
			}
			@v = ();
			$send_cmd->();
		},
		DEBUG => 0,
	);

	$fh2rw{$fh_type} = EV::io($fh_type, EV::READ, \&r_event_cb);
}



foreach my $addr (keys %addr2fh) {
	my $fh = $addr2fh{$addr};

	my @cmd = ();
	my @allcmd = ();

	my $send_cmd = sub {
		if (my $next_cmd = shift @allcmd) {
			$fh2buf{$fh} .= cmd2stream(@$next_cmd);
			push @cmd, [$$next_cmd[0]];
			$fh2ww{$fh} ||= EV::io($fh, EV::WRITE, \&w_event_cb);
		}
	};

	$fh2nodes_send_cmd{$fh} = sub {
	 	push @allcmd, \@_;
	 	$fh2buf{$fh} or $send_cmd->();
	};

	my $cmd;
	$fh2servers_reader{$fh} = get_servers_reader(
		servers => [$addr],
		sub_cmd                    => sub {
			if ($cmd = shift @cmd) {
				@$cmd;
			}
		},
		sub_response_type          => sub { my ($type) = @_ },
		sub_line_response          => sub { my ($s, $v) = @_;
			$v =~ m/^-ERR/ and die "ERROR on $$cmd[0]: ", $v;
		},
		sub_bulk_response_size     => sub { },
		sub_bulk_response_size_all => sub { },
		sub_bulk_response_arg      => sub { },
		sub_response_received      => sub {
			$send_cmd->();
			$counter_node_resp++;
			counter_decr();
		},
		DEBUG => 0,
	);

	$fh2rw{$fh} = EV::io($fh, EV::READ, \&r_event_cb);
}

sub fh_nodes_send_cmd {
	my ($addr, @args) = @_;
	foreach my $fh ($addr ? $addr2fh{$addr} : values %addr2fh) {
		$fh2nodes_send_cmd{$fh}->(@args);
		counter_incr();
		$counter_node_query++;
	}
}


fh_keys_send_cmd("SELECT", $db);
fh_type_send_cmd("SELECT", $db);

fh_nodes_send_cmd(undef, "SELECT", $db);
fh_nodes_send_cmd(undef, "FLUSHDB") if $flushdb;

fh_keys_send_cmd("KEYS", "*");


EV::loop;
