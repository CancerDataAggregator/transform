#!/usr/bin/env perl

use strict;

$| = 1;

# PARAMETERS

my $in_file = './auxiliary_metadata/__DICOM_code_maps/raw/part16.xml';

my $out_file = './auxiliary_metadata/__DICOM_code_maps/DICOM_controlled_terms.tsv';

my $num_cols = 4;

# EXECUTION

open IN, "<$in_file" or die("Can't open $in_file for reading.\n");

my $recording = 0;

my $line_count = 0;

my $in_header = 1;

my $in_para = 0;

my $current_term = '';

my $current_value = '';

my $parity = 0;

my $rows = {};

my @column_names = ();

while ( chomp( my $line = <IN> ) ) {
    
    $line_count++;

    $line =~ s/^\s+//;

    $line =~ s/\s+$//;

    if ( $line =~ /xml:id="table_D-1"/ ) {
        
        if ( $recording ) {
            
            die("FATAL: ALready recording when table tag encountered at $in_file line $line_count; aborting.\n");

        } else {
            
            $recording = 1;
        }

    } elsif ( $line =~ /<\/table/ ) {
        
        $recording = 0;

    } elsif ( $recording ) {
        
        if ( $in_header ) {
            
            if ( $line =~ /<\/thead/ ) {
                
                $in_header = 0;

            } elsif ( $in_para and $line =~ /^(.*)<\/para/ ) {
                
                my $current_value_suffix = $1;

                $current_value_suffix =~ s/^\s+//;

                $current_value_suffix =~ s/\s+$//;

                $current_value .= $current_value_suffix;

                $current_value =~ s/\<[^\>]+\>//g;

                push @column_names, $current_value;

                $in_para = 0;

                $parity++;

                $parity %= $num_cols;

            } elsif ( $in_para ) {
                
                $line =~ s/^\s+//;

                $line =~ s/\s+$//;

                $current_value .= $line;

            } elsif ( $line =~ /<para[^\>]*>(.*)$/ ) {
                
                my $content = $1;

                if ( $content =~ /<\/para/ ) {
                    
                    $content =~ s/<\/para.*$//;

                    $content =~ s/^\s+//;

                    $content =~ s/\s+$//;

                    push @column_names, $content;

                    $parity++;

                    $parity %= $num_cols;

                } else {
                    
                    $content =~ s/^\s+//;

                    $content =~ s/\s+$//;

                    $current_value = $content;

                    $in_para = 1;
                }
            }

        } else {
            
            if ( $line =~ /<td\s*[^\>]*\/>/ ) {
                
                if ( $parity == 0 ) {
                    
                    die("Unexpected failure?! line $line_count\n");

                } else {
                    
                    # No value. Save a null and move on.

                    $rows->{$current_term}->{$column_names[$parity]} = '';

                    $parity++;

                    $parity %= $num_cols;

                    $current_value = '';
                }

            } elsif ( $line =~ /<\/td>/ ) {
                
                if ( $parity == 0 ) {
                    
                    $current_term = $current_value;

                    if ( $current_term eq '' ) {
                        
                        print("Unexpected failure line $line_count\n");
                    }

                } else {
                    
                    # There are some special characters in the XML here that I don't even know how to match with REs.

                    $current_value =~ s/[^0-9a-zA-Z \.\,\/\\\<\>\?\"\:\'\;\|\]\[\}\{\=\-\+\_\)\(\*\&\^\%\$\#\@\!\~\`]//g;

                    $rows->{$current_term}->{$column_names[$parity]} = $current_value;
                }

                $parity++;

                $parity %= $num_cols;

                $current_value = '';

            } elsif ( $in_para and $line =~ /^(.*)<\/para/ ) {
                
                my $current_value_suffix = $1;

                $current_value_suffix =~ s/^\s+//;

                $current_value_suffix =~ s/\s+$//;

                $current_value .= $current_value_suffix;

                $current_value =~ s/\<[^\>]+\>//g;

                $in_para = 0;

            } elsif ( $in_para ) {
                
                $line =~ s/^\s+//;

                $line =~ s/\s+$//;

                $current_value .= $line;

            } elsif ( $line =~ /<para[^\>]*>(.*)$/ ) {
                
                my $content = $1;

                if ( $content =~ /<\/para/ ) {
                    
                    $content =~ s/<\/para.*$//;

                    $content =~ s/^\s+//;

                    $content =~ s/\s+$//;

                    $current_value .= $content;

                } else {
                    
                    $content =~ s/^\s+//;

                    $content =~ s/\s+$//;

                    $current_value .= $content;

                    $in_para = 1;
                }
            }

        } # end if ( we're in the header block )

    } # end if ( $recording )

} # end while ( chomp( my $line = <IN> ) )

close IN;

open OUT, ">$out_file" or die("Can't open $out_file for writing.\n");

print OUT join("\t", @column_names) . "\n";

foreach my $term ( sort { $a cmp $b } keys %$rows ) {
    
    print OUT "$term";

    foreach my $column_name ( @column_names[1 .. $#column_names] ) {
        
        print OUT "\t$rows->{$term}->{$column_name}";
    }

    print OUT "\n";
}

close OUT;


