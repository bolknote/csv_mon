#!/usr/bin/env perl
use Cwd;
use Term::ReadLine;
use DBI;
use Text::Tabs;

$greet = 'Welcome to the CSV SQL Monitor v1.06. Written by Stepanischev Evgeny aka BOLK. 2002.';

@SQL_commandsu = ('SELECT', 'UPDATE', 'CREATE TABLE', 'DROP TABLE', 'DELETE FROM', 'INSERT INTO', 'SHOW TABLES',
				  'BIND TABLE', 'VAR FOR');
@SQL_commandsl = map { lc } @SQL_commandsu;

@SQL_wordsu    = ('WHERE', 'FROM', 'ORDER BY', 'DISTINCT', 'VALUES', 'SET', 'DESC', 'ASC');
@SQL_wordsl = map { lc } @SQL_wordsu;

$dbh = '';
$database = @ARGV ? $ARGV[0] : cwd();

%binds = ();
%tables_attrs = ();

eval
{
	print "$greet\n";
	$term = new Term::ReadLine::Gnu;
	my $attribs=$term->Attribs;

	$attribs->{completer_quote_characters} = '"';
	$attribs->{completion_function}=
	sub
	{
		my ($text, $line, $start, $end) = @_;
		return ('eol', 'sep_char', 'quote_char', 'escape_char', 'col_names', 'file')
		if $line=~ /^VAR\s+FOR\s+\S+\s+\S*$/i;

		my $ch = substr ($line, $start, 1);

		return <$line*> if $ch eq '/' || $ch eq '.';
		my @list = map {exists $binds{$_} ? $binds{$_} : $_} $dbh->func('list_tables');

		return '* FROM' if substr ($line, $start, 1) eq '*';

		return ($start ? @SQL_wordsu : @SQL_commandsu, @list) if $ch =~ /^[A-Z]/ || $line eq '';
		return ($start ? @SQL_wordsl : @SQL_commandsl, @list) if $ch =~ /^[a-z]/;

		return @list;
	};
};

$term = new Term::ReadLine ($greet) if $@;
undef $@;

DoSmth ("u $database", '');

for ($buffer = ''; defined ($_ = -t STDIN ? $term->readline('csv> ') : <STDIN>); )
{
	$len = length;
	($com, $end) = substr ($_, $len - 1, 1) eq ';' ? (substr ($_, 0, $len-1), 'g') : split /(?=\\)\\/;

	if ($end eq '')
	{
		$buffer .= $com."\n";
	} else
	{
		chomp $buffer;
		$buffer = DoSmth ($end, $buffer.$com);

		last if $buffer eq '\\q';
	};
}

print "Bye\n";

sub DrawLine
{
	for my $w (@_)
	{
		print '+';
		print '-' x ($w + 2);
	};
	print "+\n";
};

sub DBHerr
{
	my $err;
	($err = $dbh->errstr()) =~ s!^\s*(.+?) at /.*$!$1!;
	$err =~ s/\s+$//;
	print "ERROR:\n$err.\n";
}

sub DoSmth
{
	my ($com, $str) = @_;
	my ($com, $arg) = split /\s+/, $com;

	$str =~ s/\\\\/\\/g;
	$arg =~ s/\\\\/\\/g;

	if ($com eq 'h' || $com eq '?')
	{
		print <DATA>;
	}
	elsif ($com eq 'g' || $com eq 'G')
	{
		if ($str eq '')
		{
			print "ERROR: \nNo query specified\n\n";
		} else
		{
			if ($str =~ /^VAR\s+FOR\s+(\S+)\s+(\S+)\s+(.+)$/)
			{
				my ($table, $var, $value) =  ($1, $2, $3);
				my $attrs = $tables_attrs->{"$database/$table"};
				my $svalue = $value;

				$value =~ s/\\t/\t/g;
				$value =~ s/\\n/\n/g;
				$value =~ s/\\r/\r/g;
				$value = undef if $value eq 'undef';

				if ($var eq 'col_names')
				{
					$tables_attrs->{"$database/$table"} -> {'col_names'} = [split /,\s*/, $value];
				} else
				{
					$tables_attrs->{"$database/$table"} -> {$var} = $value;
				};

				$dbh->{'csv_tables'}->{$table} = $tables_attrs->{"$database/$table"};

				print "Set successfully '$var' in '$svalue' for '$table'.\n";
				return '';
			};

			if ($str =~ /^SHOW TABLES\s*$/i)
			{	
				my @tables = map {exists $binds{$_} ? $binds{$_} : $_} $dbh->func('list_tables');
				my $header = "Tables_in_database";

				if ($com eq 'g')
				{
					my $width  = length ((sort { length ($b) <=> length ($a) } @tables)[0]);

					$width = length $header if $width < length $header;

					DrawLine $width;
					print '|'.' ' x ($width - length ($header) + 1);
					print "$header |\n";

					DrawLine $width;

					for my $t (@tables)
					{
						print "| $t".' ' x ($width - length ($t) + 1);
						print "|\n";
					}
					DrawLine $width;
				} else
				{
					for (my $i = 0; $i<@tables; $i++)
					{
						print '*' x 27, " ${\($i + 1)}. row ", '*' x 27, "\n";
						print "$header: ${tables[$i]}\n";
					};
				};

				print scalar @tables, " rows in set.\n";

				return '';
			};

			if ($str =~ /^BIND TABLE\s+(\S+)\s+(.+?)\s*$/)
			{
				undef $@;
				if ('/' eq substr $2, 0, 1)
				{
					print "Can't bind absolute address, use relative path.\n";
					return '';
				};

				$binds{$2} = $1;
				my $attrs = $dbh->{'csv_tables'}->{$1};
				${$attrs}{'file'} = $2;
				$dbh->{'csv_tables'}->{$1} = $attrs;

				print "Successfully binded.\n";
				return '';
			};

			my ($offset, $limit) = (0) x 2;
			if (/^(SELECT.+)LIMIT\s+(\d+(?:,\s*\d+)?)/)
			{
				my @limit = split /,/, $2; $str = $1;
				($offset, $limit) = @limit == 2 ? @limit : (0, $limit[0]);
			};

			if ($sth = $dbh->prepare($str))
			{
				if ($sth->execute())
				{
					my @fields =  @{$sth->{'NAME'}};
					my $rows = $sth->rows;

					if ($limit)
					{
						$rows -= $offset;
						$rows = $limit if $limit < $rows;

						$sth->fetchrow_array for (1 .. $offset);
					};

					if (@fields)
					{
						my @max_widths = map { length $_ } @fields;
						my ($i, $len, @rows, @row);

						for (my $l = $rows; $l > 0; $l--)
						{
							push @rows, @row = expand($sth->fetchrow_array);

							for ($i = 0; $i<@row; $i++)
							{					
								$len = length $row[$i];
								$max_widths[$i] = $len if $len > $max_widths[$i];
							};
						}

						if ($com eq 'g')
						{
							DrawLine @max_widths;
							for (my $f = 0; $f<@fields; $f++)
							{
								print '|';
								$len = int (($max_widths[$f] - length($fields[$f])) / 2) + 1;
								print ' ' x $len, $fields[$f], ' ' x $len;
								print ' ' if ($max_widths[$f] - length($fields[$f])) & 1;
							};
							print "|\n";
							DrawLine @max_widths;

        					for (my $f = 0; $f<$rows; $f++)
							{
								for (my $i = 0; $i<@fields; $i++)
								{
									print '|';
									$len = $max_widths[$i] - length($rows[$f * @fields + $i]) + 1;
									print ' ' x $len, $rows[$f * @fields + $i], ' ';
								};
								print "|\n";
							};

							DrawLine @max_widths;
						} else
						{
							my $width  = length ((sort { length ($b) <=> length ($a) } @fields)[0]);
							@fields = map { ' ' x ($width - length) . $_ } @fields;

        					for (my $f = 0; $f<$rows; $f++)
							{
								print '*' x 27, " ${\($f + 1)}. row ", '*' x 27, "\n";

								for (my $i = 0; $i<@fields; $i++)
								{
									print $fields[$i], ': ', $rows[$f * @fields + $i], "\n";
								};
							};
						};
					};

					print "Query OK, $rows rows proceed.\n";

					$sth->finish();
				} else
				{
					DBHerr;
				};
			} else
			{
				DBHerr;
			};
		};

		return '';
	}
	elsif ($com eq 'q')
	{
		return '\\q';
	}
	elsif ($com eq 'c')
	{
		return '';
	}
	elsif ($com eq 'r' || $com eq 'u')
	{

		$arg = cwd()."/$arg" if substr ($arg, 0, 1) ne '/';

		eval { $dbh->disconnect };
		if ($arg eq '')
		{
			if ($com eq 'u')
			{
				print "ERROR:\nUSE must be followed by a database name\n";
				return $str;
			}
		} else
		{
			$database = $arg;
		};

		undef $@;
		eval { $dbh = DBI->connect ("DBI:CSV:f_dir=$database") };
		if ($@)
		{
			print  "DBI connection error.\n";
			$dbh = '';
		} else
		{
			$dbh->{'PrintError'} = 0;
			print "Database selected.\n";
		};
	}

	return $str;
}

__DATA__

CSV SQL commands:
Note that all text commands must be first on line and end with ';'
help    (\h)    Display this help.
?       (\?)    Synonym for `help'.
clear   (\c)    Clear command.
connect (\r)    Reconnect to the server. Optional arguments are db.
go      (\g)    Send command to CSV SQL server.
ego     (\G)    Send command to CSV SQL server, display result vertically.
quit    (\q)    Quit monitor.
use     (\u)    Use another database. Takes database name as argument.
