:- use_module(library(thread)).


%extract patterns starting from whatever, ending in a TV program
% ending resource must be one of these:
% http://www.w3.org/2000/01/rdf-schema#suBClassOf http://dbpedia.org/class/yago/Program106748466
% rdf:type http://dbpedia.org/ontology/Film
% rdf:type http://dbpedia.org/ontology/TelevisionShow
% rdfs:suBClassOf http://dbpedia.org/class/yago/Entertainment100429048
%
%
%%%%%set_prolog_stack(global, limit(2*10**9)).


:- dynamic t/3.
:- dynamic done/3.
:- dynamic m/3.

%/
patterns_length2:-
	get_triples(List),
	make_patterns_length2(List,List),
	write('Finished').

patterns_length3:-
	%get_done_length3,
	get_triples(List),
	concurrent_maplist(make_patterns_length3,List),
	write('Finished').

put_done_in_db(T1,P1,T2):-
		atomic_list_concat([T1,P1,T2],'\',\'',Values),
		atom_concat('INSERT INTO \`Triples_done\` (Resource1,Property1,Resource2) VALUES(\'',Values,Part1),
		atom_concat(Part1,'\')',Query),
		between(1, 2, _),
		catch((
			db_connection(DB),
			odbc_query(DB,Query,_)),
			print('Error in DB'),fail).

		
		
get_done_length3:- 
	findall(Row,
		(db_connection(DB),
		odbc_query(DB,'SELECT Resource1,Property1,Resource2 FROM \`Triples_done\`',Row)),
		Done),
	db_disconnect,
	forall(member(row(R1,P1,R2), Done),
			assert(done(R1,P1,R2))).

get_triples(Sorted_List):-
	findall(Type1-Property1-Type2,
		(    tv_types(Object1,Type1),
		     rdf(Object1,Property1,Object2),
		     rdf(Object2,rdf:type,Type2),
		     not(rdf(_,rdfs:subClassOf,Type2)),
		     rdf_match_label(like,'http://dbpedia.org*',Type2)),
		List2),
	sort(List2,Complete_List),
	
	forall(member(A-B-C,Complete_List),
	       assert(m(A,B,C))),
	       
	findall(Type1-Property1-Type2,
		(    member(Type1-Property1-Type2,Complete_List),
		     not(done(Type1,Property1,Type2))
		     ),
		List),
	sort(List,Sorted_List)
	%length(Sorted_List, L1),
	%writeln(L1),
	%length(Complete_List,L2),
	%write(L2)
	.

make_patterns_length2(List1,List):-
	List1=[Type1-Property1-Type2|List2],
	forall(member(Type3-Property2-Type2,List),
	       (   Pattern=['2','DBpedia',Type1,Property1,Type2,Property2,Type3],
		   write(Pattern),nl,
		   count_pattern2_global_frequency(Pattern)
	       )
	      ),
	make_patterns_length2(List2,List).

make_patterns_length2([],_).

make_patterns_length3(Type1-Property1-Type2):-	
	forall((   m(Type4,Property3,Type3),
		   	   get_properties(Type2,Type3,Property2)),
	       (   Pattern=['3','DBpedia',Type1,Property1,Type2,Property2,Type3,Property3,Type4],
		 		writeln('Pattern done'),
		   		count_pattern3_global_frequency(Pattern)->true;
	       (    writeln('Property doesn\'t exist')))),
	put_done_in_db(Type1,Property1,Type2).



get_properties(Type2,Type3,Property2):-
	t(Type2,P,Type3)->
	(writeln('Trovato'),
	Property2 = P);
	(
	 rdf_optimise:rdf_optimise((rdf(Ob1,rdf:type,Type2),
				    rdf(Ob1,Property2,Ob2),
				    rdf(Ob2,rdf:type,Type3)),
				   Opt),
	 Opt,
	 assert(t(Type2,Property2,Type3))).

		 


count_pattern2_global_frequency(G) :-
	not(check_pattern_in_db(G)),!,
	G=[_,_,Resource1_type,Property1,Resource2_type,Property2,Resource3_type],
	rdf_optimise:rdf_optimise(( rdf(Movie2,rdf:type,Resource3_type),
				    rdf(Movie2,Property2,Resource2),
				    rdf(Resource2,rdf:type,Resource2_type)),Opt),
	time(aggregate_all(count,Movie2,
			   ( Opt,
			     (   rdf(Movie1,Property1,Resource2),
				 Movie1\==Movie2,
				 rdf(Movie1,rdf:type,Resource1_type)
			     ->  true
			     )
			   ),
			   C)),
	Pattern=[C,G],
	debug(film, 'Putting in DB', []),
	put_global_pattern_into_db(Pattern).

count_pattern2_global_frequency(_).

count_pattern3_global_frequency(G) :-
	not(check_pattern3_in_db(G)),!,
	G=[_,_,Resource1_type,Property1,Resource2_type,Property2,Resource3_type,Property3,Resource4_type],
	rdf(Resource1,rdf:type,Resource1_type),
	rdf(Resource1,Property1,Resource2),
	rdf(Resource2,rdf:type,Resource2_type),
	rdf(Resource2,Property2,Resource3),!,
	rdf_optimise:rdf_optimise((rdf(Resource1,Property1,Resource2),
				   rdf(Resource1,rdf:type,Resource1_type),
				   rdf(Resource4,rdf:type,Resource4_type),
				   rdf(Resource4,Property3,Resource3),
				   rdf(Resource2,rdf:type,Resource2_type),
				   rdf(Resource3,rdf:type,Resource3_type),
				   rdf(Resource2,Property2,Resource3)),
				  Opt),
	time(aggregate_all(count,Resource4,
			   ( Opt,
			     (   rdf(Resource1,Property1,Resource2),
				 Resource1\==Resource4,
				 rdf(Resource1,rdf:type,Resource1_type)
				 ->  true
			     )
			   ),
			   C)),
	Pattern=[C,G],
	debug(film, 'Putting in DB', []),
	put_global_pattern3_into_db(Pattern).

count_pattern3_global_frequency(_):-
	writeln('already in DB or does not exist').




put_global_pattern_into_db(Pattern):-
	flatten(Pattern,FP),
	atomic_list_concat(FP,'\',\'',Values),
	atom_concat('INSERT INTO \`Global patterns2\` (Frequency,Length,Provenance,Resource1,Property1,Resource2,Property2,Resource3) VALUES(\'',Values,Part1),
	atom_concat(Part1,'\')',Query),
	between(1, 2, _),
	catch((
			db_connection(DB),
			odbc_query(DB,Query,_)),
			print('Error in DB'),fail).


put_global_pattern3_into_db(Pattern):-
	flatten(Pattern,FP),
	atomic_list_concat(FP,'\',\'',Values),
	atom_concat('INSERT INTO \`Global patterns2\` (Frequency,Length,Provenance,Resource1,Property1,Resource2,Property2,Resource3,Property3,Resource4) VALUES(\'',Values,Part1),
	atom_concat(Part1,'\')',Query),
	between(1, 2, _),
	catch((
			db_connection(DB),
			odbc_query(DB,Query,_)),
			print('Error in DB'),fail).


check_pattern_in_db(Pattern):-
	Pattern=[_,_,Resource1,Property1,Resource2,Property2,Resource3],
	atom_concat('SELECT id from \`Global patterns2\` WHERE Resource1 =\'',Resource1,Q1),
	atom_concat(Q1,'\' and Property1 =\'',Q2),
	atom_concat(Q2,Property1,Q3),
	atom_concat(Q3,'\' and Resource2 =\'',Q4),
	atom_concat(Q4,Resource2,Q5),
	atom_concat(Q5,'\' and Property2 =\'',Q6),
	atom_concat(Q6,Property2,Q7),
	atom_concat(Q7,'\' and Resource3=\'',Q8),
	atom_concat(Q8,Resource3,Q9),
	atom_concat(Q9,'\'',Query),
	between(1, 2, _),
	catch((
			db_connection(DB),
			odbc_query(DB,Query,_)),
			print('Error in DB'),
			fail).

check_pattern3_in_db(Pattern):-
	Pattern=[_,_,Resource1,Property1,Resource2,Property2,Resource3,Property3,Resource4],
	atom_concat('SELECT id from \`Global patterns2\` WHERE Resource1 =\'',Resource1,Q1),
	atom_concat(Q1,'\' and Property1 =\'',Q2),
	atom_concat(Q2,Property1,Q3),
	atom_concat(Q3,'\' and Resource2 =\'',Q4),
	atom_concat(Q4,Resource2,Q5),
	atom_concat(Q5,'\' and Property2 =\'',Q6),
	atom_concat(Q6,Property2,Q7),
	atom_concat(Q7,'\' and Resource3=\'',Q8),
	atom_concat(Q8,Resource3,Q9),
	atom_concat(Q9,'\' and Property3 =\'',Q10),
	atom_concat(Q10,Property3,Q11),
	atom_concat(Q11,'\' and Resource4=\'',Q12),
	atom_concat(Q12,Resource4,Q13),
	atom_concat(Q13,'\'',Query),
	between(1, 2, _),
	catch((
			db_connection(DB),
			odbc_query(DB,Query,_)),
			print('Error in DB'),
			fail).



tv_types(Resource,'http://dbpedia.org/ontology/Film'):-
	rdf(Resource,rdf:type,'http://dbpedia.org/ontology/Film').
tv_types(Resource,'http://dbpedia.org/ontology/TelevisionShow'):-
	rdf(Resource,rdf:type,'http://dbpedia.org/ontology/TelevisionShow').
get_type(Resource, Type):-
	findall(T,
		rdf(Resource,rdf:type,T),
	       Types),
	Types=[Type|_].


:- debug(film).

:- dynamic db/1.

db_connection(DB) :-
	db(DB), !.
db_connection(DB) :-
	odbc_connect(test,DB,[user(vmo600),password('vale2703')]),
	assert(db(DB)).

db_disconnect :-
	retract(db(DB)), !,
	odbc_disconnect(DB).
db_disconnect.
