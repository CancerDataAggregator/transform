#!/usr/bin/env perl

use strict;

$| = 1;

# ARGUMENTS

my $cds_file = shift;

my $idc_file = shift;

my $out_file = shift;

die( "\n   Usage: $0 <CDS file> <IDC file> <output file>\n\n" ) if ( not -e $cds_file or not -e $idc_file or $out_file eq '' );

# PARAMETERS

my $type_map = {
    
    'aliquot' => 'sample',
    'analyte' => 'sample',
    'case' => 'case',
    'diagnosis' => 'diagnosis',
    'portion' => 'sample',
    'sample' => 'sample',
    'slide' => 'sample',

    'participant' => 'case'
};

# EXECUTION

open IN, "<$cds_file" or die("Can't open $cds_file for reading.\n");

my $header = <IN>;

my $cds_data = {};

my $cds_study_to_program = {};

my $cds_program_to_study = {};

while ( chomp( my $line = <IN> ) ) {
    
    # entity_type	entity_id	program_acronym	study_name

    my ( $entity_type, $entity_id, $program_acronym, $study_name ) = split( /\t/, $line );

    if ( exists( $type_map->{$entity_type} ) ) {
        
        my $target_type = $type_map->{$entity_type};

        $cds_data->{$target_type}->{$entity_id}->{$study_name} = 1;

        $cds_study_to_program->{$study_name} = $program_acronym;

        $cds_program_to_study->{$program_acronym}->{$study_name} = 1;
    }
}

close IN;

open IN, "<$idc_file" or die("Can't open $idc_file for reading.\n");

$header = <IN>;

my $idc_data = {};

while ( chomp( my $line = <IN> ) ) {
    
    # collection_id	submitter_case_id	idc_case_id	entity_type

    my ( $collection_id, $submitter_case_id, $idc_case_id, $entity_type ) = split( /\t/, $line );

    if ( exists( $type_map->{$entity_type} ) ) {
        
        my $target_type = $type_map->{$entity_type};

        $idc_data->{$target_type}->{$submitter_case_id}->{$collection_id} = 1;
    }
}

close IN;

my $cds_program_total_match_count = {};

my $cds_study_total_match_count = {};

my $cds_to_idc_count = {};

foreach my $target_type ( keys %$cds_data ) {
    
    foreach my $entity_id ( keys %{$cds_data->{$target_type}} ) {
        
        foreach my $study_name ( keys %{$cds_data->{$target_type}->{$entity_id}} ) {
            
            if ( exists( $idc_data->{$target_type} ) ) {
                
                if ( exists( $idc_data->{$target_type}->{$entity_id} ) ) {
                    
                    foreach my $collection_id ( keys %{$idc_data->{$target_type}->{$entity_id}} ) {
                        
#                        if ( $collection_id eq 'htan_wustl' ) {
#                            
#                            print "$target_type\t$entity_id\t$collection_id\t$study_name\n";
#                        }

                        if ( exists( $cds_to_idc_count->{$study_name}->{$collection_id} ) ) {
                            
                            $cds_study_total_match_count->{$study_name} += 1;

                            $cds_to_idc_count->{$study_name}->{$collection_id} += 1;

                        } else {
                            
                            $cds_study_total_match_count->{$study_name} = 1;

                            $cds_to_idc_count->{$study_name}->{$collection_id} = 1;
                        }

                        if ( exists( $cds_program_total_match_count->{$cds_study_to_program->{$study_name}} ) ) {
                            
                            $cds_program_total_match_count->{$cds_study_to_program->{$study_name}} += 1;

                        } else {
                            
                            $cds_program_total_match_count->{$cds_study_to_program->{$study_name}} = 1;
                        }
                    }
                }
            }
        }
    }
}

open OUT, ">$out_file" or die("Can't open $out_file for writing.\n");

print OUT join( "\t", 'match_count', 'CDS_program_acronym', 'CDS_study_name', 'IDC_collection_id' ) . "\n";

foreach my $program_acronym ( sort { $cds_program_total_match_count->{$b} <=> $cds_program_total_match_count->{$a} } keys %$cds_program_total_match_count ) {
    
    foreach my $study_name ( sort { $cds_study_total_match_count->{$b} <=> $cds_study_total_match_count->{$a} } keys %{$cds_program_to_study->{$program_acronym}} ) {
        
        foreach my $collection_id ( sort { $a cmp $b } keys %{$cds_to_idc_count->{$study_name}} ) {
            
            my $match_count = $cds_to_idc_count->{$study_name}->{$collection_id};

            print OUT join( "\t", $match_count, $program_acronym, $study_name, $collection_id ) . "\n";
        }
    }
}

close OUT;


