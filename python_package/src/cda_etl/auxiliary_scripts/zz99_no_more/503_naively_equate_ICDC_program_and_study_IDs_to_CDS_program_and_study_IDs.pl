#!/usr/bin/env perl

use strict;

$| = 1;

# ARGUMENTS

my $icdc_file = shift;

my $cds_file = shift;

my $out_file = shift;

die( "\n   Usage: $0 <ICDC file> <CDS file> <output file>\n\n" ) if ( not -e $icdc_file or not -e $cds_file or $out_file eq '' );

# PARAMETERS

my $type_map = {
    
    'case' => 'case',
    'diagnosis' => 'diagnosis',
    'sample' => 'sample',

    'participant' => 'case'
};

# EXECUTION

open IN, "<$icdc_file" or die("Can't open $icdc_file for reading.\n");

my $header = <IN>;

my $icdc_data = {};

my $icdc_study_name_to_id = {};

my $icdc_study_to_program = {};

my $icdc_program_to_study = {};

while ( chomp( my $line = <IN> ) ) {
    
    # entity_type	entity_id	program_acronym	study_name	study_id

    my ( $entity_type, $entity_id, $program_acronym, $study_name, $study_id ) = split( /\t/, $line );

    if ( exists( $type_map->{$entity_type} ) ) {
        
        my $target_type = $type_map->{$entity_type};

        $icdc_data->{$target_type}->{$entity_id}->{$study_name} = 1;

        $icdc_study_name_to_id->{$study_name} = $study_id;

        $icdc_study_to_program->{$study_name} = $program_acronym;

        $icdc_program_to_study->{$program_acronym}->{$study_name} = 1;
    }
}

close IN;

open IN, "<$cds_file" or die("Can't open $cds_file for reading.\n");

$header = <IN>;

my $cds_data = {};

my $cds_study_name_to_id = {};

my $cds_study_to_program = {};

my $cds_program_to_study = {};

while ( chomp( my $line = <IN> ) ) {
    
    # entity_type	entity_id	program_acronym	study_name	study_id

    my ( $entity_type, $entity_id, $program_acronym, $study_name, $study_id ) = split( /\t/, $line );

    if ( exists( $type_map->{$entity_type} ) ) {
        
        my $target_type = $type_map->{$entity_type};

        $cds_data->{$target_type}->{$entity_id}->{$study_name} = 1;

        $cds_study_name_to_id->{$study_name} = $study_id;

        $cds_study_to_program->{$study_name} = $program_acronym;

        $cds_program_to_study->{$program_acronym}->{$study_name} = 1;
    }
}

close IN;

my $icdc_program_total_match_count = {};

my $icdc_study_total_match_count = {};

my $icdc_to_cds_count = {};

foreach my $target_type ( keys %$icdc_data ) {
    
    foreach my $entity_id ( keys %{$icdc_data->{$target_type}} ) {
        
        foreach my $icdc_study_name ( keys %{$icdc_data->{$target_type}->{$entity_id}} ) {
            
            if ( exists( $cds_data->{$target_type} ) ) {
                
                if ( exists( $cds_data->{$target_type}->{$entity_id} ) ) {
                    
                    foreach my $cds_study_name ( keys %{$cds_data->{$target_type}->{$entity_id}} ) {
                        
                        if ( exists( $icdc_to_cds_count->{$icdc_study_name}->{$cds_study_name} ) ) {
                            
                            $icdc_study_total_match_count->{$icdc_study_name} += 1;

                            $icdc_to_cds_count->{$icdc_study_name}->{$cds_study_name} += 1;

                        } else {
                            
                            $icdc_study_total_match_count->{$icdc_study_name} = 1;

                            $icdc_to_cds_count->{$icdc_study_name}->{$cds_study_name} = 1;
                        }

                        if ( exists( $icdc_program_total_match_count->{$icdc_study_to_program->{$icdc_study_name}} ) ) {
                            
                            $icdc_program_total_match_count->{$icdc_study_to_program->{$icdc_study_name}} += 1;

                        } else {
                            
                            $icdc_program_total_match_count->{$icdc_study_to_program->{$icdc_study_name}} = 1;
                        }
                    }
                }
            }
        }
    }
}

open OUT, ">$out_file" or die("Can't open $out_file for writing.\n");

print OUT join( "\t", 'match_count', 'ICDC_program_acronym', 'ICDC_study_name', 'ICDC_study_id', 'CDS_program_acronym', 'CDS_study_name', 'CDS_study_id' ) . "\n";

foreach my $icdc_program_acronym ( sort { $icdc_program_total_match_count->{$b} <=> $icdc_program_total_match_count->{$a} } keys %$icdc_program_total_match_count ) {
    
    foreach my $icdc_study_name ( sort { $icdc_study_total_match_count->{$b} <=> $icdc_study_total_match_count->{$a} } keys %{$icdc_program_to_study->{$icdc_program_acronym}} ) {
        
        foreach my $cds_study_name ( sort { $a cmp $b } keys %{$icdc_to_cds_count->{$icdc_study_name}} ) {
            
            my $match_count = $icdc_to_cds_count->{$icdc_study_name}->{$cds_study_name};

            my $cds_program_acronym = $cds_study_to_program->{$cds_study_name};

            my $icdc_study_id = $icdc_study_name_to_id->{$icdc_study_name};

            my $cds_study_id = $cds_study_name_to_id->{$cds_study_name};

            print OUT join( "\t", $match_count, $icdc_program_acronym, $icdc_study_name, $icdc_study_id, $cds_program_acronym, $cds_study_name, $cds_study_id ) . "\n";
        }
    }
}

close OUT;


