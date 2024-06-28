#!/usr/bin/env perl

use strict;

$| = 1;

# ARGUMENTS

my $icdc_file = shift;

my $pdc_file = shift;

my $out_file = shift;

die( "\n   Usage: $0 <ICDC file> <PDC file> <output file>\n\n" ) if ( not -e $icdc_file or not -e $pdc_file or $out_file eq '' );

# PARAMETERS

my $type_map = {
    
    'aliquot' => 'sample',
    'case' => 'case',
    'diagnosis' => 'diagnosis',
    'sample' => 'sample'
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

open IN, "<$pdc_file" or die("Can't open $pdc_file for reading.\n");

$header = <IN>;

my $pdc_data = {};

my $pdc_study_to_project_and_program = {};

while ( chomp( my $line = <IN> ) ) {
    
    # program_name	project_submitter_id	study_submitter_id	pdc_study_id	entity_submitter_id	entity_id	entity_type

    my ( $program_name, $project_submitter_id, $study_submitter_id, $pdc_study_id, $entity_submitter_id, $entity_id, $entity_type ) = split( /\t/, $line );

    if ( exists( $type_map->{$entity_type} ) ) {
        
        my $target_type = $type_map->{$entity_type};

        $pdc_data->{$target_type}->{$entity_submitter_id}->{$pdc_study_id} = 1;

        $pdc_study_to_project_and_program->{$pdc_study_id}->{$study_submitter_id}->{$project_submitter_id}->{$program_name} = 1;
    }
}

close IN;

my $icdc_program_total_match_count = {};

my $icdc_study_total_match_count = {};

my $icdc_to_pdc_count = {};

foreach my $target_type ( keys %$icdc_data ) {
    
    foreach my $entity_id ( keys %{$icdc_data->{$target_type}} ) {
        
        foreach my $study_name ( keys %{$icdc_data->{$target_type}->{$entity_id}} ) {
            
            if ( exists( $pdc_data->{$target_type} ) ) {
                
                if ( exists( $pdc_data->{$target_type}->{$entity_id} ) ) {
                    
                    foreach my $pdc_study_id ( keys %{$pdc_data->{$target_type}->{$entity_id}} ) {
                        
                        if ( exists( $icdc_to_pdc_count->{$study_name}->{$pdc_study_id} ) ) {
                            
                            $icdc_study_total_match_count->{$study_name} += 1;

                            $icdc_to_pdc_count->{$study_name}->{$pdc_study_id} += 1;

                        } else {
                            
                            $icdc_study_total_match_count->{$study_name} = 1;

                            $icdc_to_pdc_count->{$study_name}->{$pdc_study_id} = 1;
                        }

                        if ( exists( $icdc_program_total_match_count->{$icdc_study_to_program->{$study_name}} ) ) {
                            
                            $icdc_program_total_match_count->{$icdc_study_to_program->{$study_name}} += 1;

                        } else {
                            
                            $icdc_program_total_match_count->{$icdc_study_to_program->{$study_name}} = 1;
                        }
                    }
                }
            }
        }
    }
}

open OUT, ">$out_file" or die("Can't open $out_file for writing.\n");

print OUT join( "\t", 'match_count', 'ICDC_program_acronym', 'ICDC_study_name', 'ICDC_study_id', 'PDC_program_name', 'PDC_project_submitter_id', 'PDC_study_submitter_id', 'PDC_pdc_study_id' ) . "\n";

foreach my $program_acronym ( sort { $icdc_program_total_match_count->{$b} <=> $icdc_program_total_match_count->{$a} } keys %$icdc_program_total_match_count ) {
    
    foreach my $study_name ( sort { $icdc_study_total_match_count->{$b} <=> $icdc_study_total_match_count->{$a} } keys %{$icdc_program_to_study->{$program_acronym}} ) {
        
        my $study_id = $icdc_study_name_to_id->{$study_name};

        foreach my $pdc_study_id ( sort { $a cmp $b } keys %{$icdc_to_pdc_count->{$study_name}} ) {
            
            my $match_count = $icdc_to_pdc_count->{$study_name}->{$pdc_study_id};

            foreach my $study_submitter_id ( sort { $a cmp $b } keys %{$pdc_study_to_project_and_program->{$pdc_study_id}} ) {
                
                foreach my $project_id ( sort { $a cmp $b } keys %{$pdc_study_to_project_and_program->{$pdc_study_id}->{$study_submitter_id}} ) {
                    
                    foreach my $program_name ( sort { $a cmp $b } keys %{$pdc_study_to_project_and_program->{$pdc_study_id}->{$study_submitter_id}->{$project_id}} ) {
                        
                        print OUT join( "\t", $match_count, $program_acronym, $study_name, $study_id, $program_name, $project_id, $study_submitter_id, $pdc_study_id ) . "\n";
                    }
                }
            }
        }
    }
}

close OUT;


