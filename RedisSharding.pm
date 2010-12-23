
package RedisSharding;

use strict;
use warnings;

use Exporter;
our @ISA = qw(Exporter);
our @EXPORT = qw(cmd2stream get_client_reader get_servers_reader);



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
	
	my $sub_clean = sub {
		($cmd, @s) = ();
		%cmd = ();
		$reply_type = "";
		%bulk_args     = ();
		%bulk_args_had = ();
		%bulk_size     = ();
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
 
			if ($reply_type =~ m/^(multi_)?bulk$/ and @s eq keys %bulk_args) {
				$args{sub_bulk_response_size_all}->(1);
			}
			if (@s eq keys %cmd) {
				print "RESPONSE from all on $cmd\n" if $args{DEBUG};
				$args{sub_response_received}->(1);
				$sub_clean->();
				$next = 1;
			}

			redo NEXT if $next;
		}
	};
}


1;
