#!/usr/bin/env perl

use strict;

$| = 1;

# ARGUMENTS

my $gc_file = shift;

my $pdc_file = shift;

my $out_file = shift;

die( "\n   Usage: $0 <GC file> <PDC file> <output file>\n\n" ) if ( not -e $gc_file or not -e $pdc_file or $out_file eq '' );

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

open IN, "<$gc_file" or die("Can't open $gc_file for reading.\n");

my $header = <IN>;

my $gc_data = {};

my $gc_study_name_to_phs_accession = {};

my $gc_study_name_to_uuid = {};

my $gc_study_to_program = {};

my $gc_program_to_study = {};

while ( chomp( my $line = <IN> ) ) {
    
    # program.uuid	program.program_acronym	program.program_name	study.uuid	study.phs_accession	study.study_acronym	study.study_name	entity_submitter_id	entity_id	entity_type

    my ( $program_id, $program_acronym, $program_name, $study_id, $study_phs_accession, $study_acronym, $study_name, $entity_submitter_id, $entity_id, $entity_type ) = split( /\t/, $line );

    if ( exists( $type_map->{$entity_type} ) ) {
        
        my $target_type = $type_map->{$entity_type};

        $gc_data->{$target_type}->{$entity_submitter_id}->{$study_name} = 1;

        $gc_study_name_to_phs_accession->{$study_name} = $study_phs_accession;

        $gc_study_name_to_uuid->{$study_name} = $study_id;

        $gc_study_to_program->{$study_name} = $program_acronym;

        $gc_program_to_study->{$program_acronym}->{$study_name} = 1;
    }
}

close IN;

open IN, "<$pdc_file" or die("Can't open $pdc_file for reading.\n");

$header = <IN>;

my $pdc_data = {};

my $pdc_study_to_project_and_program = {};

while ( chomp( my $line = <IN> ) ) {
    
    # program.program_id	program.program_submitter_id	program.name	project.project_id	project.project_submitter_id	project.name	study.study_id	study.study_submitter_id	study.pdc_study_id	entity_submitter_id	entity_id	entity_type

    my ( $program_id, $program_submitter_id, $program_name, $project_id, $project_submitter_id, $project_name, $study_id, $study_submitter_id, $pdc_study_id, $entity_submitter_id, $entity_id, $entity_type ) = split( /\t/, $line );

    if ( exists( $type_map->{$entity_type} ) ) {
        
        my $target_type = $type_map->{$entity_type};

        $pdc_data->{$target_type}->{$entity_submitter_id}->{$pdc_study_id} = 1;

        $pdc_study_to_project_and_program->{$pdc_study_id}->{$study_submitter_id}->{$project_submitter_id}->{$program_name} = 1;
    }
}

close IN;

my $gc_program_total_match_count = {};

my $gc_study_total_match_count = {};

my $gc_to_pdc_count = {};

foreach my $target_type ( keys %$gc_data ) {
    
    foreach my $entity_id ( keys %{$gc_data->{$target_type}} ) {
        
        foreach my $study_name ( keys %{$gc_data->{$target_type}->{$entity_id}} ) {
            
            if ( exists( $pdc_data->{$target_type} ) ) {
                
                if ( exists( $pdc_data->{$target_type}->{$entity_id} ) ) {
                    
                    foreach my $pdc_study_id ( keys %{$pdc_data->{$target_type}->{$entity_id}} ) {
                        
                        if ( exists( $gc_to_pdc_count->{$study_name}->{$pdc_study_id} ) ) {
                            
                            $gc_study_total_match_count->{$study_name} += 1;

                            $gc_to_pdc_count->{$study_name}->{$pdc_study_id} += 1;

                        } else {
                            
                            $gc_study_total_match_count->{$study_name} = 1;

                            $gc_to_pdc_count->{$study_name}->{$pdc_study_id} = 1;
                        }

                        if ( exists( $gc_program_total_match_count->{$gc_study_to_program->{$study_name}} ) ) {
                            
                            $gc_program_total_match_count->{$gc_study_to_program->{$study_name}} += 1;

                        } else {
                            
                            $gc_program_total_match_count->{$gc_study_to_program->{$study_name}} = 1;
                        }
                    }
                }
            }
        }
    }
}

open OUT, ">$out_file" or die("Can't open $out_file for writing.\n");

print OUT join( "\t", 'match_count', 'GC_program_acronym', 'GC_study_name', 'GC_study_phs_accession', 'GC_study_uuid', 'PDC_program_name', 'PDC_project_submitter_id', 'PDC_study_submitter_id', 'PDC_pdc_study_id' ) . "\n";

foreach my $program_acronym ( sort { $gc_program_total_match_count->{$b} <=> $gc_program_total_match_count->{$a} } keys %$gc_program_total_match_count ) {
    
    foreach my $study_name ( sort { $gc_study_total_match_count->{$b} <=> $gc_study_total_match_count->{$a} } keys %{$gc_program_to_study->{$program_acronym}} ) {
        
        my $study_phs_accession = $gc_study_name_to_phs_accession->{$study_name};

        my $study_uuid = $gc_study_name_to_uuid->{$study_name};

        foreach my $pdc_study_id ( sort { $a cmp $b } keys %{$gc_to_pdc_count->{$study_name}} ) {
            
            my $match_count = $gc_to_pdc_count->{$study_name}->{$pdc_study_id};

            foreach my $study_submitter_id ( sort { $a cmp $b } keys %{$pdc_study_to_project_and_program->{$pdc_study_id}} ) {
                
                foreach my $project_id ( sort { $a cmp $b } keys %{$pdc_study_to_project_and_program->{$pdc_study_id}->{$study_submitter_id}} ) {
                    
                    foreach my $program_name ( sort { $a cmp $b } keys %{$pdc_study_to_project_and_program->{$pdc_study_id}->{$study_submitter_id}->{$project_id}} ) {
                        
                        print OUT join( "\t", $match_count, $program_acronym, $study_name, $study_phs_accession, $study_uuid, $program_name, $project_id, $study_submitter_id, $pdc_study_id ) . "\n";
                    }
                }
            }
        }
    }
}

close OUT;


